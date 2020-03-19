import pysftp
import pymysql
import csv
import stat
import os
#from datetime import date
#import sys
#from watchdog.observers import Observer
#from watchdog.events import PatternMatchingEventHandler

#cnopts = pysftp.CnOpts()
#cnopts.hostkeys = None
#srv = pysftp.Connection(host="", port=, username="", password="",
                      #  cnopts=cnopts)

#class MyHandler(PatternMatchingEventHandler):
 #   patterns = ["*.csv"]

  #  def process(self, event):
   #     """
    #    event.event_type
     #       'modified' | 'created' | 'moved' | 'deleted'
      #  event.is_directory
       #     True | False
       # event.src_path
        #    /Daily/2019/8/
       # """
        # the file will be processed there
        #print event.src_path, event.event_type  # print now only for degug
       # srv.get(event.src_path, 'C:/Users/RR/Desktop/hello.csv')
        #db = pymysql.connect('localhost', 'root', 'enc@12345', 'testdb')
       # cursor = db.cursor()
        #try:
         #   cursor.execute('CREATE TABLE details(MOBILE NUMBER INT, EVENT DATE VARCHAR2(10), TIME VARCHAR2(12), NETWORK ID CHAR(1), SYSTEM CHAR(1),\
          #       DIALLED VARCHAR2(15), DURATION INT, VOLUME INT, Cost FLOAT, COUNTRY CHAR(3), NETWORK CHAR(2), CHARGE METHOD CHAR(1), TARIFF \
           #      INT, ZONE CODE CHAR(4), SERVICECODE INT, Prebundle Cost FLOAT, DIRECTION INT, VAT Status VARCHAR2(10), Charge Code VARCHAR2(30))')
      #  except:
       #     pass

        #try:
         #   data = csv.reader(file('Mobile_20190801_OCI92316_Calls.csv'))
          #  for row in data:
           #     cursor.execute('insert into details(MOBILE NUMBER, EVENT DATE, TIME, NETWORK ID, SYSTEM, DIALLED, DURATION, VOLUME,\
            #        Cost, COUNTRY, NETWORK, CHARGE METHOD, TARIFF, ZONE CODE, SERVICECODE, Prebundle Cost, DIRECTION, VAT Status, Charge\
             #       Code) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', row)
      #  except Exception as e:
       #     print(e)
        #    pass

        #db.commit()
        #cursor.close()

    #def on_modified(self, event):
        #self.process(event)

    #def on_created(self, event):
     #   self.process(event)

#if __name__ == '__main__':
#    args = sys.argv[1:]
#    observer = Observer()
#    observer.schedule(MyHandler(), path=args[0] if args else '.')
#    observer.start()

#    try:
#        while True:
#            time.sleep(1)
#    except KeyboardInterrupt:
#        observer.stop()

#    observer.join()


def download_csv():
    try:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(host="dwp.digitalwholesalesolutions.com", port=2222, username="OCI92316", password="xUNLDogxM", cnopts=cnopts)
        print('Connection Established')
    except:
        print('Connection Error')
        return
    try:
        directory = '/Daily/'
        dir_y = srv.listdir(directory).pop()
        dir_m = srv.listdir(directory + dir_y + '/').pop()
        dir = directory + dir_y + '/' + dir_m + '/'
        file = srv.listdir_attr(dir)
        path= 'C:/Users/RR/Desktop/Data/'
        for f in file:
            if '.csv' in f.filename:
                print("Checking %s..." % f.filename)
                if ((not os.path.isfile('C:/Users/RR/Desktop/Data/'+f.filename)) or
                        (f.st_mtime > os.path.getmtime('C:/Users/RR/Desktop/Data/'+f.filename))):
                    print("Downloading %s..." % f.filename)
                    srv.get(dir+f.filename, path+f.filename)
                    try:
                        mysql(f.filename)
                    except Exception as e:
                        print(e)
                        os.remove('C:/Users/RR/Desktop/Data/' + f.filename)
        #for f in file:
         #   if '.csv' in f:
          #      srv.get(dir + str(f), 'C:/Users/RR/Desktop/Data/' + f)
           #     mysql(f)
        #today= date.today()
        #if (str(today).partition('-')[2].partition('-')[2]=='01' or str(today).partition('-')[2].partition('-')[2]=='02' or str(today).partition('-')[2].partition('-')[2]=='03'):
        #    directory = '/Daily/'+str(today.year)+'/'+str(today.month)+'/'
            # srv.get("/Daily/2019/8/Mobile_20190801_OCI92316_Calls.csv", 'C:/Users/RR/Desktop/Mobile_20190801_OCI92316_Calls.csv')
        #    file = srv.listdir(directory)
        #    for f in file:
        #        if '.csv' in f:
        #            srv.get(directory+f, 'C:/Users/RR/Desktop/Data/'+f)
        #            mysql(f)
    #directory= ['/Daily/2019/6/', '/Daily/2019/7/', '/Daily/2019/8/']
    except Exception as e:
        print('FTP:'+str(e))
        pass

    #print('File Downloaded')
    srv.close()


def clean(row):
    r= []
    for cell in row:
        cell = cell.strip()
        r.append(cell)
    return r



def mysql(file):
    db= pymysql.connect('localhost', 'root', 'enc12345', 'testdb')
    cursor= db.cursor()
    try:
        cursor.execute("""CREATE TABLE details(MOBILE_NUMBER VARCHAR(60), EVENT_DATE VARCHAR(60), TIME VARCHAR(60), NETWORK_ID VARCHAR(60),
         PSYSTEM VARCHAR(60), DIALLED VARCHAR(60), DURATION VARCHAR(60), VOLUME VARCHAR(60), Cost VARCHAR(60), COUNTRY VARCHAR(60),
          NETWORK VARCHAR(60), CHARGE_METHOD VARCHAR(60), TARIFF VARCHAR(60), ZONE_CODE VARCHAR(60), SERVICECODE VARCHAR(60),
           Prebundle_Cost VARCHAR(60), DIRECTION VARCHAR(60), VAT_Status VARCHAR(60), Charge_Code VARCHAR(60))""")
        #print('Table Created')
    except Exception as e:
        #print(e)
        pass

    try:
        with open('C:/Users/RR/Desktop/Data/'+file, 'r') as w:
            data= csv.reader(w)
            next(data)
            for row in data:
                try:
                    r= clean(row)
                    cursor.execute("""insert into details(MOBILE_NUMBER, EVENT_DATE, TIME, NETWORK_ID, PSYSTEM, DIALLED,
                           DURATION, VOLUME, Cost, COUNTRY, NETWORK, CHARGE_METHOD, TARIFF, ZONE_CODE, SERVICECODE, 
                           Prebundle_Cost, DIRECTION, VAT_Status, Charge_Code) values(%s, %s, %s, %s, %s, %s, %s, 
                           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", r)
                except Exception as e:
                    #print(e)
                    continue
            #print('Table Updated')
    except Exception as e:
        print('Database: '+str(e))
        pass

    db.commit()
    cursor.close()
    #os.remove('C:/Users/RR/Desktop/Data/'+file)

if __name__=='__main__':
    download_csv()

