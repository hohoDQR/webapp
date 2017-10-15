import logging

import aiomysql
import asyncio


@asyncio.coroutine
def crete_pool(db, user, loop, minsize=1, maxsize=10, autocommit=True, charset='utf-8', password=None, port=3306,
               host='localhost', **kw):
    logging.info("create database connnection pool...")
    global __pool
    __pool = yield from aiomysql.create_pool(
        host =host,
        port =port,
        user =user,
        passwd =password,
        db =db,
        charset =charset,
        autocommit =autocommit,
        maxsize=maxsize,
        minsize =minsize,
        loop = loop
    )

@asyncio.coroutine
def select(sql, args, size = None):
    logging.log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield  from cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('row returned: %s' %len(rs))
        return  rs
