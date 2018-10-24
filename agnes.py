# Database analyses for all databases
import os
from sqlalchemy import create_engine
import pandas as pd
__version__ = "1.3.9"


class Agnes:
    
    #connect to mysql
    def myconnect(self, uname, passw, host_name, db_name, tb_name):
        try:
            mcon = create_engine("mysql+mysqldb://"+"%s"%uname+":"+"%s"%passw+"@"+"%s"%host_name+"/"+"%s"%db_name)
            sq_tb = pd.read_sql('%s' %tb_name, mcon)
        except Exception:
            print("Ensure connections details are correct.")
            return
        df1 = pd.DataFrame(sq_tb)
        return df1
        
    #connect to postgresql    
    def pconnect(self, uname, passw, host_name, db_name, tb_name):
        try:
            pcon = create_engine("postgresql://"+"%s"%uname+":"+"%s"%passw+"@"+"%s"%host_name+":5432/"+"%s"%db_name)
            ptable = pd.read_sql('select * from '+ ' %s ' % tb_name, pcon)
        except Exception:
            print("Ensure connections details are correct")
            return
        df2 = pd.DataFrame(ptable)
        return df2
        
    #connect to oracle
    def oconnector(self, uname, passw, host_name, db_name, tb_name):
        try:
            ocon = create_engine("oracle://"+"%s"%uname+":"+"%s"%passw+"@"+"%s"%host_name+":1521/"+"%s"%db_name)
            otable = pd.read_sql("%s" %tb_name, ocon)
        except Exception:
            print("Ensure connections details are correct.")
            return
        df3 = pd.DataFrame(otable)
        return df3

    #connect to mssql
    def mss_connector(self, db_name, tb_name):
        try:
            mss = create_engine("mssql+pyodbc://"+"%s"%db_name)
            mss_table = pd.read_sql("%s" %tb_name, mss)
        except Exception:
            print("Ensure connection details are correct.")
            return
        df4 = pd.DataFrame(mss_table)
        return df4

    #connect to sqlite
    def mylite(self, db_name, tb_name):
        try:
            slite = create_engine("sqlite:///"+"%s"%db_name+".db")
            lite_tb = pd.read_sql("%s" %tb_name, slite)
        except Exception:
            print("Ensure connection details are correct.")
            return
        df5 = pd.DataFrame(lite_tb)
        return df5

    #save db table to excel, csv, json sheet, prompts user for new name if name already exists 
    def saver(self, df):
        fname = input("Enter name to save: ")
        xname, cname, jname = fname +'.xlsx', fname + '.csv', fname+ '.json'
        if not os.path.isfile(xname):
            fs = df.to_excel(xname)
        elif not os.path.isfile(cname):
            fs = df.to_csv(cname)
        elif not os.path.isfile(jname):
            fs = df.to_json(jname)
        else: 
            print("[*] %s, %s, %s already exists..." %(xname, cname, jname))
            return Agnes.saver(self, df)
        
    #data analyses
    def db_analyses(self, df):
        print("Statistics: %s" %df.describe())
        print("Head: %s"  %df.head())
        print("Tail: %s"  %df.tail())
        print("Correlation: %s" %df.corr())
        print("Covarriance: %s" %df.cov())
        print("Kurt: %s" %df.kurt())
        print("Skew: %s" %df.skew())
        print("Summation: %s" %df.sum())
        print("Maximum: %s" %df.max())
        print("Minimum: %s" %df.min())
    
    #combine two database tables    
    def merger(self, df1, df2):
        df_meg = pd.merge(df1, df2)
        return df_meg

if __name__ == '__main__':
    Agnes()
