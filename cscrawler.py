# -*- coding: cp1252 -*-
import urlparse
import urllib2
import cookielib
import urllib
import requests
import re
import mechanize
from mechanize._opener import urlopen
from mechanize._form import ParseResponse
import bs4
import time
import psycopg2
import sys
from db import *
import datetime
from datetime import date

Soup = bs4.BeautifulSoup

URLFB    = "https://www.facebook.com"
URLCS    = "https://www.couchsurfing.org"
USERNAME = 'username'
PASSWORD = 'password'
MAX      = 100000000

#lista persone da visitare
tovisit = []
#dizionario persone visitate
visited = {}

#connessione al database
con = None
con = connect('','','')
create_table(con)

#recupera la lista degli utenti da visitare
data = restore(con)

#popola la lista tovisit
if data:
  #ripristina la sessione precedente
  print 'restoring previous session...'
  for link in data :
    tovisit.append(link[0])
  print 'session restored'
else :
  #inizia a popolare la lista delle persone da visitare
  url = "https://www.couchsurfing.org/search/in/everywhere/mode/L"
  page = urllib2.urlopen(url)
  lista_primi = Soup(page.read())
  #prende i link
  first = lista_primi.findAll('span', attrs={'class' : 'result_username'})
  first_time = 0

  #inserisce i primi link nella lista
  for link in first :
    peopl = link.findNext('a')['href']
    tovisit.append(peopl.split('&')[0])


#Browser
br = mechanize.Browser()
cj = cookielib.LWPCookieJar()
br.set_cookiejar(cj)

br.set_handle_equiv(True)
br.set_handle_gzip(True)
br.set_handle_redirect(True)
br.set_handle_referer(True)
br.set_handle_robots(False)

br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)

# Want debugging messages?
#br.set_debug_http(True)
#br.set_debug_redirects(True)
#br.set_debug_responses(True)

# User-Agent (this is cheating, ok?)
br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]


# Login in FB
request = br.open(URLCS)
response = br.response()
#print response.read()
forms = mechanize.ParseResponse(response, backwards_compat=False)

# Username and Password are stored in this form
form = forms[1]

form["username"] = USERNAME
form["password"] = PASSWORD

#fa il login
br.open(form.click()).read()

#outfile_neg = open("negative.txt", "a")
#outfile_neu = open("neutral.txt", "a")
outfile_log = init_log()

extracted_neg = 0
extracted_neu = 0
extracted_pos = 0

examined = 0

saved = 0

while len(tovisit) and extracted_neg < MAX:

  user = tovisit[0]
  examined = examined + 1

  if extracted_neg % 10 == 0 and saved == 0:
    print "Saving"
#    outfile_neg.close()
#    outfile_neu.close()
#    outfile_neg = open("negative.txt", "a")
#    outfile_neu = open("neutral.txt", "a")
    saved = 1

  print "Checking the reviews of "+user+" ("+str(examined)+"\\"+str(len(tovisit))+")"

  tovisit.pop(0)
  currentURL = urlparse.urljoin(URLCS, user)
  done = False

  for i in range(1, 3):
    try:
      print 'Opening page ' + currentURL
      response = br.open(currentURL)
      print 'Done'
      done = True
      break
    except:
      if currentURL is not None:
        print 'Error loading page ' + currentURL
      else:
        print 'Error loading unknown page'
        continue
      time.sleep(60*i)

  if not done:
    if currentURL is not None:
      log_write(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')+'\tI give up on loading ' + currentURL +'\n')
      print 'I give up on loading ' + currentURL
    else:
      log_write(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')+'\tI give up loading unknown page \n')
      print 'I give up loading unknown page'
    continue
    
  #print response.read()
  #load all the references
  link_more = br.links(text_regex="all references")
  done = True
  
  for link in link_more :
    done = False
    print "Loading all references"
    for i in range(1, 3):
      try:
        response = br.follow_link(link)
        print 'Done'
        done = True
        break
      except:
        if hasattr(link, "href") :
          print 'Error loading page' + link['href']
        else:
          print 'Error loading unknown page'
          continue
        time.sleep(60*i)

    if not done:
      if hasattr(link, "href") :
        log_write(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')+'\tI give up on loading ' + link['href'] + '\n')
        print 'I give up on loading ' + link['href']
      else:
        log_write(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')+'\tI give up on loading unknown page \n')
        print 'I give up on loading unknown page' 
      continue

  soup = Soup(response.read())

  #estrae il nome
  name = soup.find('h1', attrs={'class' : 'profile'})
  if name is not None:
    #rimuove gli spazi
    membername = ' '.join(name.contents[0].lstrip().split())

  #estrae i dati relativi all'utente quali nazionalita e citta dall'intestazione
  nazionalita = soup.find(name='a', attrs={'target' : 'mapblast'})
  if nazionalita is not None:
    #espressione regolare per separare nazione da citta
    nazionalita = re.sub( '(?<!^)(?=[A-Z])', '_', nazionalita.text )
    citta = nazionalita
    #salvo la nazione che e la prima stringa
    nazionalita = nazionalita.split('_')[0]
    #salvo la citta che e alla fine della stringa
    try:
      citta = citta.split('_')[2]
    except:
      try:
        citta = citta.split('_')[1]
      except:
      	citta = citta.split('_')[0]

  table = soup.find('table', attrs={'class':'generalinfo'})
  age = 0
  gender = 'Unknown'
  regDate = ''
  if table is not None :
    rows = table.findAll('tr')
    for tr in rows:
      #membername since parsing
      if tr.text.find('member since') >= 0:
        cols = tr.findAll('td')
        for td in cols:
          regDate = ''.join(td.find(text=True))
        regDate = regDate.replace('th','')
        regDate = regDate.replace('st','')
        regDate = regDate.replace('rd','')
        regDate = regDate.replace('nd','')
        regDate = regDate.replace('Augu', 'Aug')
      #age parsing
      if tr.find(text=re.compile('^age$')) >= 0:
        cols = tr.find('td')
        age = ''.join(cols.find(text=True))
      # parsing gender
      if tr.text.find('gender') >= 0:
        cols = tr.find('td')
        gender = ''.join(cols.find(text=True))
        
      # controlla che sia effettivamente il sesso
      if gender.find("Male")<0 and gender.find("Female")<0 :
        gender = "Unknown"

  #genera l'id dell'utente corrente
  userid = user
  if userid[0]=='/' :
    userid = userid[1:]
  if userid[len(userid)-1]=='/':
    userid = userid[0:len(userid)-1]

  list_tag = soup.findAll(attrs={'class' : 'reference_from'})
  list_tag.extend(soup.findAll(attrs={'class' : 'refnotIRL'}))

  print 'Parsing the references'

  for tag in list_tag:
 
    tag_item = tag.findNext(name='a', attrs={'class' : 'userlink'})

    if tag_item is not None:

      reviewer = tag_item['href']

      #genera l'id dell'utente
      reviewerid = reviewer
      if reviewerid[0]=='/' :
        reviewerid = reviewerid[1:]
      if reviewerid[len(reviewerid)-1]=='/':
        reviewerid = reviewerid[0:len(reviewerid)-1]

      #aggiunge alla lista tutti i riferimenti trovati in reference from
      if reviewer not in visited:
        tovisit.append(reviewer)

      update(con,userid,age,regDate,'TRUE')

      #estrae e salva i commenti negativi
      if tag.text.find('Negative') >= 0 :
        #estrae la data del voto
        dataVoto = tag.find('sup')
        if dataVoto is not None:
          dataVoto = dataVoto.text
        else:
          break
        if dataVoto.find(',') < 0 :
          dataVoto = dataVoto+','+' '+ str(date.today().year)
        #estrae la nazione
        nazione = tag.find('small')
        
        if nazione is not None:
          nazione = (nazione.text).replace(' ','-')
          #cancella il carattere di spazio html
          nazione = nazione.replace(u'\xa0', '|')
            
          #estrae la citta del revisionato
          cittaRev = nazione.split(',')[0]
          cittaRev = cittaRev[1:].replace('-',' ')

          #estrae la nazione del revisionato
          nazione = nazione.split(',')[1]
          nazione = nazione.split('|')[0]
          nazione = nazione[1:].replace('-',' ')

          tag_it = tag_item.text+'\n'+cittaRev+'\n'+nazione

         
        #dizionario con una tupla (nome revisore,nome revisionato) come chiave e voto come valore
        visited[(tag_it,membername+'\n'+citta+'\n'+nazionalita)]='-1 \n'+ dataVoto
        
        #inserisce i record nelle tabelle
        insert_nazione(con,nazionalita)
        insert_nazione(con,nazione)
        insert_citta(con,citta,nazionalita)
        insert_citta(con,cittaRev,nazione)
        insert_utente(con,userid,membername,age,citta,nazionalita,regDate,gender)
        insert_utente(con,reviewerid,tag_item.text,0,cittaRev,nazione,None,None)
        insert_commento(con,reviewerid,userid,-1,dataVoto,tag.p.text.encode('ascii', 'ignore')+'\n')
        update_flag(con,'TRUE',userid)

        
#        outfile_neg.write(user+'\t'+reviewer+'\t')
#        outfile_neg.write('Negative\t')
#        outfile_neg.write(tag.p.text.encode('ascii', 'ignore')+'\n')
        extracted_neg = extracted_neg + 1
        print "Found " + str(extracted_neg) + " negative reviews"
        saved = 0
        
      #estrae e salva i commenti neutri
      elif tag.text.find('Positive') < 0 :
        #estrae la data del voto
        dataVoto = tag.find('sup')
        if dataVoto is not None:
          dataVoto = dataVoto.text
        else:
          break
        if dataVoto.find(',') < 0 :
          dataVoto = dataVoto + ',' + ' ' + str(date.today().year)
        #estrae la nazione del revisionato
        nazione = tag.find('small')
          
        if nazione is not None:
          nazione = (nazione.text).replace(' ','-')
          #cancella il carattere di spazio html
          nazione = nazione.replace(u'\xa0', '|')
            
          #estrae la citta del revisionato
          cittaRev = nazione.split(',')[0]
          cittaRev = cittaRev[1:].replace('-',' ')

          #estrae la nazione del revisionato
          nazione = nazione.split(',')[1]
          nazione = nazione.split('|')[0]
          nazione = nazione[1:].replace('-',' ')

          tag_it = tag_item.text+'\n'+cittaRev+'\n'+nazione

        #dizionario con una tupla (nome revisore,nome revisionato) come chiave e voto come valore
        visited[(tag_it,membername+'\n'+citta+'\n'+nazionalita)]='0 \n'+ dataVoto

        #inserisce i record nelle tabelle
        insert_nazione(con,nazione)
        insert_nazione(con,nazionalita)
        insert_citta(con,citta,nazionalita)
        insert_citta(con,cittaRev,nazione)
        insert_utente(con,userid,membername,age,citta,nazionalita,regDate,gender)
        insert_utente(con,reviewerid,tag_item.text,0,cittaRev,nazione,None,None)
        insert_commento(con,reviewerid,userid,0,dataVoto,tag.p.text.encode('ascii', 'ignore')+'\n')
        update_flag(con,'TRUE',userid)

#        outfile_neu.write(user+'\t'+reviewer+'\t')
#        outfile_neu.write('Neutral\t')
#        outfile_neu.write(tag.p.text.encode('ascii', 'ignore')+'\n')
        extracted_neu = extracted_neu + 1
        print "Found " + str(extracted_neu) + " neutral reviews"
        saved = 0

      #estrae e salva i commenti positivi
      else :
        #estrae la data del voto
        dataVoto = tag.find('sup')
        if dataVoto is not None:
          dataVoto = dataVoto.text
        else:
          break
        if dataVoto.find(',') < 0 :
          dataVoto = dataVoto + ',' + ' ' + str(date.today().year)
        #estrae la nazione del revisionato
        nazione = tag.find('small')
          
        if nazione is not None:
          nazione = (nazione.text).replace(' ','-')
          #cancella il carattere di spazio html
          nazione = nazione.replace(u'\xa0', '|')
            
          #estrae la citta del revisionato
          cittaRev = nazione.split(',')[0]
          cittaRev = cittaRev[1:].replace('-',' ')

          #estrae la nazione del revisionato
          nazione = nazione.split(',')[1]
          nazione = nazione.split('|')[0]
          nazione = nazione[1:].replace('-',' ')

          tag_it = tag_item.text+'\n'+cittaRev+'\n'+nazione

        #dizionario con una tupla (nome revisore,nome revisionato) come chiave e voto come valore
        visited[(tag_it,membername+'\n'+citta+'\n'+nazionalita)]='0 \n'+ dataVoto

        #inserisce i record nelle tabelle
        insert_nazione(con,nazione)
        insert_nazione(con,nazionalita)
        insert_citta(con,citta,nazionalita)
        insert_citta(con,cittaRev,nazione)
        insert_utente(con,userid,membername,age,citta,nazionalita,regDate,gender)
        insert_utente(con,reviewerid,tag_item.text,0,cittaRev,nazione,None,None)
        insert_commento(con,reviewerid,userid,1,dataVoto,tag.p.text.encode('ascii', 'ignore')+'\n')
        update_flag(con,'TRUE',userid)

        #outfile_neu.write(user+'\t'+reviewer+'\t')
        #outfile_neu.write('Neutral\t')
        #outfile_neu.write(tag.p.text.encode('ascii', 'ignore')+'\n')
        extracted_pos = extracted_pos + 1
        print "Found " + str(extracted_pos) + " positive reviews"
        saved = 0

  #extracted_neg = MAX
  #print 'Done'
  print 'Done'
  time.sleep(1.5)


#disegna il grafo
#draw_digraph(visited)
print "Finished"

#print visited.keys()

outfile_log.close()
#outfile_neg.close()
#outfile_neu.close()
