#!/usr/local/bin/python3.7
#import sqlite3
import pysqlite3 as sqlite3
import random
import cgi
import json

# パラメタ解析

urlparams = cgi.FieldStorage()

num = 100
if (len(urlparams.getlist('num')) > 0) and (urlparams.getlist('num')[0].isdecimal()):
  num = int(urlparams.getlist('num')[0])
  if num > 10000:
    num = 10000

condflag = False
condlist = [0, '', 0, '', 0, '', 0, '', 0, '']
tmp = ['pref', 'city', 'sex', 'age', 'job']
for i in range(5):
  if (len(urlparams.getlist(tmp[i])) > 0):
    condflag = True
    condlist[i * 2] = 1
    condlist[i * 2 + 1] = urlparams.getlist(tmp[i])[0]

# SQLite3接続
con = sqlite3.connect('japanesetable.db')
cur = con.cursor()

results = []
if condflag:
  # 下準備
  #   条件に合った行の累積度数を計算してインデクスも張っておく
  statement = '''
    CREATE TEMP TABLE TMPTABLE(PREF TEXT, CITY TEXT, SEX TEXT, AGE TEXT, JOB TEXT, POPULATION INT, CUMULATIVEPOPULATION INT)
    '''
  cur.execute(statement)
  statement = '''
    CREATE INDEX POPINDEX ON TMPTABLE (CUMULATIVEPOPULATION)
    '''
  cur.execute(statement)
  statement = '''
    INSERT INTO TMPTABLE(PREF, CITY, SEX, AGE, JOB, POPULATION, CUMULATIVEPOPULATION)
    SELECT
      PREF, CITY, SEX, AGE, JOB, POPULATION,
      SUM(POPULATION) over (ORDER BY PREF, CITY, SEX, AGE, JOB) AS CUMULATIVEPOPULATION
    FROM JAPANESETABLE
    WHERE
      (0=? OR PREF=?) AND (0=? OR CITY=?) AND (0=? OR SEX=?) AND (0=? OR AGE=?) AND (0=? OR JOB=?)
    '''
  cur.execute(statement, condlist)

  # 本番
  statement = '''
    WITH RANDOMNUM(C) AS (SELECT ABS(RANDOM() % SUM(POPULATION)) FROM TMPTABLE)
    SELECT PREF, CITY, SEX, AGE, JOB /*, POPULATION, CUMULATIVEPOPULATION */ FROM TMPTABLE,RANDOMNUM
    WHERE CUMULATIVEPOPULATION >= RANDOMNUM.C
    ORDER BY CUMULATIVEPOPULATION ASC
    LIMIT 1
    '''
  for cnt in range(num):
    for i in cur.execute(statement):
      results.append({
        'pref': i[0],
        'city': i[1],
        'sex': i[2],
        'age': i[3],
        'job': i[4],
      })
else:
  # 無条件は既に累積度数が計算してインデクスも付いているので実行するだけ
  statement = '''
    SELECT PREF, CITY, SEX, AGE, JOB /*, POPULATION, CUMULATIVEPROBABILITY */ FROM JAPANESETABLE
    WHERE CUMULATIVEPROBABILITY >= ?
    ORDER BY CUMULATIVEPROBABILITY ASC
    LIMIT 1
    '''
  for cnt in range(num):
    for i in cur.execute(statement, (random.random(),)):
      results.append({
        'pref': i[0],
        'city': i[1],
        'sex': i[2],
        'age': i[3],
        'job': i[4],
      })
con.close
print('Content-Type: application/json; charset=utf-8\n')
print(json.dumps(results, ensure_ascii=False))