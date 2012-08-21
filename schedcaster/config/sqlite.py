import sqlite3 as sqlite
import types
import threading
import schedcaster.scheduler as Scheduler


class Config(object):
    '''Config provider for Scheduler service, that uses SQLite to store
       schedules'''
    # should be used, because sqlite3 connections are not thread-safe
    __connectionPool = {}  # threadHash -> connection

    def __init__(self, filename):
        self.filename = filename
        # can't use persistent connection, because it is not thread-safe
        #self.connection = sqlite.connect(filename)

        self.__makeTables()

    def __requireConnection(commit=True, close=False):
        '''Decorator, which provides valid connectiob object to the
           decorated function. Original function call fn(self, a, b, c) will
           be relaced with fn(self, connection, a, b, c). Connction wll
           autocommit its changes and close automatically if other behavior
           is not specified
           :param commit: whether to autocommit changes after the function call
           :param close: whether to close connection after the call'''
        def decorator(fn):
            def withConnection(self, *args, **kwargs):
                selfClass = self.__class__
                # doesn't work for some reason
                #threadHash = hash(threading.currentThread())
                threadHash = threading.currentThread()
                connection = None
                if (threadHash in selfClass.__connectionPool and\
                  selfClass.__connectionPool[threadHash] != None):
                    connection = selfClass.__connectionPool[threadHash]
                else:
                    connection = sqlite.connect(self.filename)
                    connection.text_factory = str
                    selfClass.__connectionPool[threadHash] = connection
                ret = fn(self, connection, *args, **kwargs)
                if commit:
                    connection.commit()
                if close:
                    connection.close()
                    selfClass.__connectionPool[threadHash] = None
                return ret
            return withConnection
        return decorator

    @__requireConnection()
    def clear(self, connection):
        '''Removes all schedules from the db'''
        cursor = connection.cursor()
        cursor.execute("""delete from tbl_sched;""")
        cursor.execute("""delete from tbl_sched_args;""")

    def saveOrUpdate(self, entry):
        '''Saves an object, if it doesn't exists else updates
           existing object
           :param entry: object to save'''
        if entry.id == None:
            raise RuntimeError("entry must have a unique id")
        if not self.__entryExists(entry):
            self.save(entry)
        else:
            self.update(entry)

    @__requireConnection()
    def save(self, connection, entry):
        '''Save a new entry and update input object's id accordingly
           :param entry: entry to save'''
        cursor = connection.cursor()
        cursor.execute("""insert into tbl_sched
            (id, cron, state, name, handler, status)
            values
            (?, ?, ?, ?, ?, ?)""",
            (entry.id, entry.cron, entry.state, entry.name,
            entry.handler, entry.status))
        cursor = connection.cursor()
        cursor.execute("""select (last_insert_rowid());""")
        #id = cursor.fetchone()[0]

        #entry.id = id

        for arg in entry.args.values():
            cursor = connection.cursor()
            cursor.execute("""insert into tbl_sched_args
                (source_id, name, value)
                values
                (?, ?, ?);""",
                (entry.id, arg.name, arg.value))

    @__requireConnection()
    def update(self, connection, entry):
        '''Update an existing entry
        :param entry: entry to update'''
        if entry.id == None:
            raise RuntimeError("Can't update an entry without a key!")

        cursor = connection.cursor()
        cursor.execute("""update tbl_sched
            set
                cron=?, state=?, name=?, handler=?, status=?
            where id=?""",
            (entry.cron, entry.state, entry.name, entry.handler, entry.status,
            entry.id))

        for arg in entry.args.values():
            cursor = connection.cursor()
            cursor.execute("""select * from tbl_sched_args
                where source_id=? and name=?""",
                (entry.id, arg.name))
            try:
                next(cursor)
                cursor = connection.cursor()
                if isinstance(arg.value, list):
                    pass
                else:
                    cursor.execute("""update tbl_sched_args
                        set
                            value=?
                        where source_id=? and name=?""",
                        (arg.value, entry.id, arg.name))
            except StopIteration:
                cursor = connection.cursor()
                if type(arg.value) == types.ListType:
                    pass
                else:
                    cursor.execute("""insert into tbl_sched_args
                        (source_id, name, value)
                        values
                        (?, ?, ?)""",
                        (entry.id, arg.name, arg.value))

    @__requireConnection()
    def get(self, connection, id=None):
        '''Get:
           a) all entries if id = None
           b) some entries if id is a list pr a tuple of ids (e.g. id=[1,2,3])
           c) one entry if id is an integer (e.g. id=3)
           Independent of input parameters, method always returns an _array_ of
           entries.
           :param id: int, [int] or None, specifying the criteria of fetching
           :rtype: list of int'''
        cursor = connection.cursor()
        if id == None:
            cursor.execute("""select id, cron, state, name, handler, status
                from tbl_sched;""")
        elif type(id) == types.ListType or type(id) == types.TupleType:
            # hack, because '?' placeholder doesn't support lists
            # see http://stackoverflow.com/questions/7418849/
            cursor.execute("""select id, cron, state, name, handler, status
                from tbl_sched where id in (%s)""" %
                ",".join(('?',) * len(id)), id)
        else:
            cursor.execute("""select id, cron, state, name, handler, status
                from tbl_sched where id=?""", (id,))

        return self.__makeEntries(cursor)

    @__requireConnection()
    def getActive(self, connection):
        '''Gets all entries with their DONE mark unset.
           :rtype: list of int'''
        cursor = connection.cursor()
        cursor.execute("""select id, cron, state, name, handler, status
            from tbl_sched where (state & 1) = 0""")

        return self.__makeEntries(cursor)

    @__requireConnection()
    def __makeTables(self, connection):
        cursor = connection.cursor()
        cursor.execute("""create table if not exists tbl_sched
        (id blob,
         cron varchar,
         state integer,
         name varchar,
         handler varchar,
         status text,
         primary key (id));""")
        cursor.execute("""create table if not exists tbl_sched_args
        (source_id integer,
         name varchar,
         value text,
         primary key (source_id, name));""")

    @__requireConnection()
    def __entryExists(self, connection, entry):
        cursor = connection.cursor()
        cursor.execute("select id from tbl_sched where id=?", (entry.id,))
        try:
            next(cursor)
            return True
        except StopIteration:
            return False

    def __makeEntries(self, cursor):
        entries = []
        while True:
            row = cursor.fetchone()
            if row == None:
                break
            entry = self.__makeEntry(row)
            entries.append(entry)

        return entries

    @__requireConnection()
    def __makeEntry(self, connection, row):
        entry = Scheduler.Entry(*row)

        cursor = connection.cursor()
        cursor.execute("""select name, value from tbl_sched_args
            where source_id=?""", (entry.id,))
        arrays = {}
        while True:
            rowArg = cursor.fetchone()
            if rowArg == None:
                break

            if len(rowArg[0]) > 1 and rowArg[0][0] != ':' and \
               ":" in rowArg[0][0]:
                pair = rowArg[0][0].split(":", 2)
                handle = None
                if pair[0] in arrays:
                    handle = arrays[pair[0]]
                else:
                    handle = Scheduler.Arg(entry, pair[0], [])
                    arrays[pair[0]] = handle
                if pair[1] == '':
                    handle.value.append(rowArg[1])
                else:
                    # try to build simple array if possible, else revert to map
                    try:
                        handle.value[int(pair[1])] = rowArg[1]
                    except ValueError:
                        handle.value[pair[1]] = rowArg[1]
            else:
                arg = Scheduler.Arg()
                arg.source = entry
                arg.name = rowArg[0]
                arg.value = rowArg[1]
                entry.args[arg.name] = arg

        for array in arrays.values():
            entry.args[arg.name] = arg

        return entry
