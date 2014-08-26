import sqlite3
import sys
sys.path.insert(0,"/home/mgadbois/xport/")

import xport

postfix = { 1999: "",
        2001: "_b",
        2003: "_c",
        2005: "_d",
         }

demo_fields = [ "SEQN", "RIDAGEYR", "RIAGENDR" ]
dxx_fields = [ "DXXTRFAT", "DXDTOFAT", "DXDTOLE" ]


sql = sqlite3.connect("nhanes.sqlite")
k = postfix.keys()
k.sort()
for year in k:
    print year
        
    demo_fn = "DEMO" + postfix[year].upper() + ".XPT"
    dxx_fn = "dxx" + postfix[year].lower() + ".xpt"
    demo_xpt = xport.XportReader(demo_fn)
    dxx_xpt = xport.XportReader(dxx_fn)
    if year == 1999:
        sql.execute(""" DROP TABLE IF EXISTS demo; """)
        sql.execute(""" DROP TABLE IF EXISTS dxx; """)
        s = """ CREATE TABLE demo (SEQN INTEGER PRIMARY KEY, CYCLE INTEGER,""" + ", ".join([ x["name"] + " REAL" for x in demo_xpt.fields if x["name"] != "SEQN" ]) + ");"
        sql.execute(s)
        s = """ CREATE TABLE dxx (SEQN, CYCLE INTEGER,""" + ", ".join([ x["name"] + " REAL" for x in dxx_xpt.fields if x["name"] != "SEQN" ]) + ");"
        sql.execute(s)
    for n in demo_xpt:
        try:
            del n["AIALANG"]
            del n["FIALANG"]
            del n["MIAPROXY"]
            del n["FIAPROXY"]
            del n["SIAINTRP"]
            del n["SIALANG"]
            del n["FIAINTRP"]
            del n["MIAINTRP"]
            del n["SIAPROXY"]
            del n["MIALANG"]
            del n["DMDFMSIZ"]
        except:
            pass
        q = """ INSERT INTO demo (CYCLE,{}) VALUES ({},{});""".format(",".join(n.keys()),year,",".join(["?"] * len(n.keys())))
        try:
            sql.execute(q,tuple(n.values()))
           # sql.commit()
        except:
            print q,n.values()
            raise

    for n in dxx_xpt:
        q = """ INSERT INTO dxx (CYCLE,{}) VALUES ({},{});""".format(",".join(n.keys()),year,",".join(["?"] * len(n.keys())))
        try:
            sql.execute(q,tuple(n.values()))
          #  sql.commit()
        except:
            print q,n.values()
            raise
    sql.commit()
            
  #  for n in demo_xpt:
        
   #     sql.execute(""" INSERT INTO demo (SEQN, CYCLE, %s ) VALUE (%s); """, (
    

