import apscheduler.scheduler as apscheduler
import logging

STATE_DONE = 1
STATE_ONESHOT = 2


class Scheduler:
    def __init__(self, config, grace_time=None):
        self.config = config
        self.grace_time = grace_time or 60 * 60 * 24  # 1 day
        self.scheduler_real = apscheduler.Scheduler(
            misfire_grace_time=self.grace_time)
        apscheduler.logger = logging
        self.handlers = {}

    def addHandler(self, name, handler):
        self.handlers[name] = handler

    def removeHandler(self, name):
        if name in self.handlers:
            self.handlers.pop(name)

    def start(self, refresh=True):
        if self.started():
            return
        try:
            self.scheduler_real.start()
            if refresh:
                self.refresh(False)
        except ValueError as e:
            logging.error(\
              "[BUG] failed to add unknown scheduler entry: %s" % str(e))

    def stop(self):
        if not self.started():
            return
        self.scheduler_real.shutdown(shutdown_threadpool=False)
        # a hack, because apscheduler doesn't clears its jobs list
        # even with shutdown(..., close_jobstores=True)
        self.scheduler_real = apscheduler.Scheduler(
            misfire_grace_time=self.grace_time)

    def refresh(self, restart=True):
        if self.started() and restart:
            self.stop()
        if restart:
            self.start(False)
        self.__reschedule()

    def __reschedule(self):
        for entry in self.config.getActive():
            try:
                sjob = []

                # a hack (entry=entry) to avoid lexical passing of object
                # see: http://stackoverflow.com/questions/233673
                #
                # using reference type for sjob to modify it further in the
                # code if sjob = 0 was used, it would be copied here and
                # issuing sjob = self.sceduler_real... later would be of no use
                def doProcess(entry=entry, sjob=sjob):
                    if not entry.state & STATE_DONE:
                        self.__process(entry)
                        if entry.state & STATE_ONESHOT:
                            entry.state |= STATE_DONE
                            self.config.update(entry)
                            try:
                                self.scheduler_real.unschedule_job(sjob[0])
                            except KeyError:
                                # apscheduler automatically unshecules
                                # one-shot cron jobs, so unshedule_job will
                                # fail in this case and raise KeyError,
                                # we can't know if job is unscheduled,
                                # but we need to unschedule one-shots with
                                # non-one-shot crons (e.g. * * * * * * *)
                                # that is done above, in the 'try' block
                                pass
                sjob.append(self.scheduler_real.add_cron_job(doProcess,
                                             **self.__cronToAPMap(entry.cron)))
            except ValueError as e:
                # if job is scheduled at the past and it was not done before,
                # issue it to be done immediately
                if str(e) != 'Not adding job since it would never be run':
                    raise
                if    entry.state & STATE_ONESHOT and\
                  not entry.state & STATE_DONE:
                    entry.cron = '* * * * * * *'
                    sjob.append(self.scheduler_real.add_cron_job(doProcess,
                                             **self.__cronToAPMap(entry.cron)))

    def __process(self, entry):
        if entry.handler in self.handlers:
            self.handlers[entry.handler](**self.__argsToMap(entry.args))

    def __argsToMap(self, args):
        theMap = {}
        for arg in args.values():
            if len(arg.name) < 1 or arg.name[0] == ':':
                break
            theMap[arg.name] = arg.value
        return theMap

    def __cronToAPMap(self, cron):
        theMap = {}
        split = str(cron).split(" ")
        theMap['second'] = split[0]
        theMap['minute'] = split[1]
        theMap['hour'] = split[2]
        theMap['day'] = split[3]
        theMap['month'] = split[4]
        theMap['year'] = split[5]
        theMap['day_of_week'] = split[6]
        return theMap

    def started(self):
        return self.scheduler_real.running


class Entry(object):
    def __init__(self, id=None,
                 cron="*/15 * * * * * *",
                 state=0,
                 name="",
                 handler="",
                 status="",
                 args=None):
        self.id = id
        self.cron = cron
        self.state = state
        self.name = name
        self.handler = handler
        self.status = status
        # can't set default args to {}, because that would create static list,
        # that would be shared between Entry objects and thus would contain
        # incorrect info
        if args == None:
            self.args = {}  # a small hack (see above)
        else:
            self.args = args

    def arg(self, *args, **kwargs):
        arg = Arg(self, *args, **kwargs)
        self.args[arg.name] = arg


class Arg(object):
    source = None
    name = None
    value = None

    def __init__(self, source=None, name="in", value=""):
        self.source = source
        self.name = name
        self.value = value