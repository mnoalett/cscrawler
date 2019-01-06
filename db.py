import urllib
import requests
import psycopg2
import sys
import time
import datetime
import re

#file di log
def init_log() :
    global outfile_log
    outfile_log = open("log.txt", "a")
    outfile_log.close()
    return outfile_log

#scrive nel file di log
def log_write(string) :
    outfile_log = open("log.txt", "a")
    outfile_log.write(string)
    outfile_log.close()
    

#funzione che si connette al database postgresql
def connect(db,user,pwd) :
    con = None
    try:
      con = psycopg2.connect(database=db, user=user, password=pwd)
      return con
    except psycopg2.DatabaseError, e:
      print 'Error %s' % e
      sys.exit(1)                         

def connect1(db, user, pwd):
    con = None
    try:
      con = psycopg2.connect(database=db, user=user, password=pwd)
      cur = con.cursor()
      #crea le tabelle del database
      cur.execute("DROP TABLE IF EXISTS Utenti CASCADE")
      cur.execute("DROP TABLE IF EXISTS Commemti CASCADE")
      cur.execute("CREATE TABLE Utenti (ID VARCHAR(50) PRIMARY KEY references Commenti(Da), Nome VARCHAR(50), Nazionalita VARCHAR(20), Citta VARCHAR(30))")
      cur.execute("CREATE TABLE Recensioni (Da VARCHAR(50) references Utenti(ID),A VARCHAR(50) references Utenti(ID), Voto smallint, data VARCHAR(10), Testo text)")
      con.commit()
      return con
    except psycopg2.DatabaseError, e:
      print 'Error %s' % e
      sys.exit(1)

#funzione che crea le tabelle del database qualora non esistano
def create_table(dbcon):
    con = dbcon
    try:
      cur = con.cursor()
      #crea le tabelle del database
      cur.execute("CREATE TABLE IF NOT EXISTS nazionalita (nazionalita_id  serial, nazionalita varchar(80) unique not null, primary key(nazionalita_id));")
      cur.execute("select count(*) from pg_class where relname='nazionalita' and relkind='r'")
      rows = cur.fetchone()
      if rows[0] <= 0 :
          cur.execute("CREATE INDEX nazionalitaIDX1 ON nazionalita(nazionalita);")
      cur.execute("CREATE TABLE IF NOT EXISTS citta (citta_id serial, citta varchar(80) not null, nazionalita_id  int not null, primary key(citta_id), foreign key(nazionalita_id) references nazionalita(nazionalita_id));")
      cur.execute("select count(*) from pg_class where relname='citta' and relkind='r'")<=0
      rows = cur.fetchone()
      if rows[0] <= 0 :
          cur.execute("CREATE INDEX IF NOT EXISTS cittaIDX1 ON citta(citta);")
      cur.execute("CREATE TABLE IF NOT EXISTS utente (utente_id serial, cs_id varchar(100) unique not null, visitato bool default false, nome varchar(80), eta int2, citta_id int, registrazione date, primary key(utente_id), foreign key(citta_id) references citta(citta_id));")
      cur.execute("select count(*) from pg_class where relname='utente' and relkind='r'")<=0
      rows = cur.fetchone()
      if rows[0] <= 0 :
          cur.execute("CREATE INDEX utenteIDX1 ON utente(cs_id);")
      cur.execute("CREATE TABLE IF NOT EXISTS commento (commento_id  serial, autore int not null, ospite int not null, data date not null, commento text, voto int2, primary key(commento_id), unique(autore,ospite,data), foreign key(autore) references utente(utente_id), foreign key(ospite) references utente(utente_id), CHECK(voto BETWEEN -1 AND 1) );")
      cur.execute("select count(*) from pg_class where relname='commento' and relkind='r'")<=0
      rows = cur.fetchone()
      if rows[0] <= 0 :
          cur.execute("CREATE INDEX commentoIDX1 ON commento(autore); CREATE INDEX commentoIDX2 ON commento(ospite);")
      con.commit()
      return con
    except psycopg2.DatabaseError, e:
      print 'Error %s' % e
      sys.exit(1)

#funzione che inserisce i valori sulla nazione
def insert_nazione(dbcon,nazione):
    con = dbcon

    try :
        cur = con.cursor()
        #controlla se il record esiste gia
        cur.execute("SELECT Count(*) FROM nazionalita WHERE nazionalita=%(nazione)s", {"nazione": nazione})
        rows = cur.fetchone()
        #se non esistite lo inserisce
        if rows[0]==0 :    
            cur.execute("INSERT INTO nazionalita (nazionalita) VALUES (%(nazione)s)", {"nazione": nazione})
            con.commit()
    except psycopg2.DatabaseError, e:
      print 'Error %s' % e
      sys.exit(1)

#funzione che inserisce i valori sulla citta
def insert_citta(dbcon,citta,nazione):
    con = dbcon
    cur = con.cursor()
    cur.execute("SELECT nazionalita_id FROM nazionalita WHERE nazionalita=%(nazione)s", {"nazione": nazione})
    nazione_id = cur.fetchone()
    nazione_id = nazione_id[0]
    
    query = "INSERT INTO citta (citta,nazionalita_id) VALUES (%s,%s);"
    
    data = (citta,nazione_id)

    try :
        cur = con.cursor()
        #controlla se il record esiste gia
        cur.execute("SELECT Count(*) FROM citta WHERE citta=%(citta)s AND nazionalita_id= %(nazione_id)s", {"citta": citta, "nazione_id": nazione_id})
        rows = cur.fetchone()
        #se non esistite lo inserisce
        if rows[0]<=0 :    
            cur.execute(query,data)
            con.commit()
    except psycopg2.DatabaseError, e:
      print 'Error %s' % e
      sys.exit(1)
      
#funzione che inserisce i valori su un utente nel database
def insert_utente(dbcon, userid, nome, eta, citta, nazione, registrazione, gender):
    con = dbcon
    cur = con.cursor()
    cur.execute("SELECT nazionalita_id FROM nazionalita WHERE nazionalita=%(nazione)s", {"nazione": nazione})
    nazione_id = cur.fetchone()[0]
    citta_id = cur.execute("SELECT citta_id FROM citta WHERE nazionalita_id=%(nazione_id)s AND citta=%(citta)s", {"nazione_id": nazione_id, "citta": citta})
    citta_id = cur.fetchone()[0]
    
    query1 = "INSERT INTO utente (cs_id, nome, eta, citta_id) VALUES (%s,%s,%s,%s);"
    query2 = "INSERT INTO utente (cs_id, nome, eta, citta_id, registrazione, sesso) VALUES (%s,%s,%s,%s,%s,%s);"
    data1 = (userid,nome,eta,citta_id)
    data2 = (userid,nome,eta,citta_id,registrazione,gender)
    
    try :
        cur = con.cursor()
        #controlla se il record esiste gia
        cur.execute("SELECT Count(*) FROM utente WHERE cs_id=%(userid)s", {"userid": userid})
        rows = cur.fetchone()
        #se non esistite lo inserisce
        if rows[0]==0 :
            if registrazione is not None and gender is not None:
                cur.execute(query2,data2)
                con.commit()
            else :
                cur.execute(query1,data1)
                con.commit()
            
        #altrimenti se necessario fai un update            
        else :
            if eta != 0 :
                cur.execute("UPDATE utente SET eta=%(eta)s WHERE cs_id=%(userid)s",{"eta":eta, "userid":userid})
                con.commit()
            if registrazione is not None:
                cur.execute("UPDATE utente SET registrazione=%(registrazione)s WHERE cs_id=%(userid)s",{"registrazione":registrazione, "userid":userid})
                con.commit()
            if gender is not None:
                cur.execute("UPDATE utente SET sesso=%(sesso)s WHERE cs_id=%(userid)s",{"sesso":gender, "userid":userid})
                con.commit()
            
    except psycopg2.DatabaseError, e:
      log_write(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')+'\tError %s' % e +'\n')
      print 'Error %s' % e
      sys.exit(1)

def update(dbcon,userid,eta,registrazione,flag):
    con = dbcon
    try:
        cur = con.cursor()
	if eta is not None:
	    cur.execute("UPDATE utente SET eta=%(eta)s WHERE cs_id=%(userid)s",{"eta":eta, "userid":userid})
	    con.commit()
	if registrazione is not None:
	    cur.execute("UPDATE utente SET registrazione=%(registrazione)s WHERE cs_id=%(userid)s",{"registrazione":registrazione,"userid":userid})
	    con.commit()
	cur.execute("UPDATE utente SET visitato=%(flag)s WHERE cs_id=%(userid)s",{"flag":flag, "userid":userid})
	con.commit()
    except:
    	log_write(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')+'\tError %s' % e +'\n')
        print 'Error %s' % e
        sys.exit(1)

def insert_utenti2(dbcon,userid,nome,citta,nazione):
    con = dbcon
    query = "INSERT INTO Utenti (ID, Nome, Nazionalita, Citta) VALUES (%s,%s,%s,%s);"
    data = (userid,nome,citta,nazione)
    
    try :
        cur = con.cursor()
        #controlla se il record esiste gia
        cur.execute("SELECT Count(*) FROM utente WHERE ID='"+userid+"'")
        rows = cur.fetchone()
        #se non esistite lo inserisce
        if rows[0]==0 :    
            cur.execute(query,data)
            con.commit()
            
    except psycopg2.DatabaseError, e:
      print 'Error %s' % e
      sys.exit(1)
      
#funzione che inserisce i valori nella tabella commenti
def insert_commento(dbcon,da,a,voto,data,testo) :
    con = dbcon

    cur = con.cursor()
    cur.execute("SELECT utente_id FROM utente WHERE cs_id='"+da+"'")
    da = cur.fetchone()[0]
    cur.execute("SELECT utente_id FROM utente WHERE cs_id='"+a+"'")
    a = cur.fetchone()[0]
    
    query = "INSERT INTO commento (autore, ospite, data, commento, voto) VALUES (%s,%s,%s,%s,%s);"
    data = (da,a,data,testo,voto)
    
    try :
        #controlla se il record esiste gia
        cur.execute("SELECT Count(*) FROM commento WHERE autore=%(da)s AND ospite=%(a)s", {"da":da, "a":a})
        rows = cur.fetchone()
        #se non esistite lo inserisce
        if rows[0]==0 :
            cur.execute(query,data)
            con.commit()
    except psycopg2.DatabaseError, e:
      print 'Error %s' % e
      sys.exit(1)

def insert_commento1(dbcon,da,a,voto,data,testo,permanenza,meet) :
    con = dbcon

    cur = con.cursor()
    cur.execute("SELECT utente_id FROM utente WHERE cs_id='"+da+"'")
    da = cur.fetchone()[0]
    cur.execute("SELECT utente_id FROM utente WHERE cs_id='"+a+"'")
    a = cur.fetchone()[0]
    
    query = "INSERT INTO commento (autore, host, data, commento, voto, permanenza, meet) VALUES (%s,%s,%s,%s,%s, %s, %s);"
    data = (da,a,data,testo,voto,permanenza,meet)
    
    try :
        #controlla se il record esiste gia
        cur.execute("SELECT Count(*) FROM commento WHERE autore=%(da)s AND host=%(a)s", {"da":da, "a":a})
        rows = cur.fetchone()
        #se non esistite lo inserisce
        if rows[0]==0 :
            cur.execute(query,data)
            con.commit()
    except psycopg2.DatabaseError, e:
      print 'Error %s' % e
      sys.exit(1)

#update tabella utenti
def update_flag(dbcon, flag, userid) :
    con = dbcon
    cur = con.cursor()
    try :
        cur.execute("UPDATE utente SET visitato=%(flag)s WHERE cs_id=%(userid)s", {"flag":flag, "userid":userid})
        con.commit()
    except psycopg2.DatabaseError, e:
      print 'Error %s' % e
      sys.exit(1)

#ripristina la sessione precedente
def restore(dbcon) :
    con = dbcon
    cur = con.cursor()
    try :
        cur.execute("SELECT cs_id FROM utente where visitato=FALSE")
        data = cur.fetchall()
    except psycopg2.DatabaseError, e:
      print 'Error %s' % e
      sys.exit(1)
    return data
