import requests
import xmltodict
import pandas as pd
import md5
import MySQLdb
import ConfigParser
from uuid import UUID

propertiesFile = "my.properties"
cp = ConfigParser.ConfigParser()
cp.readfp(open(propertiesFile))

dbhost = cp.get('DB', 'host')
dbuser = cp.get('DB', 'user')
dbpassword = cp.get('DB', 'password')
dbname = cp.get('DB', 'db')

url = 'https://losangeles.craigslist.org/search/reb?format=rss&is_paid=all&max_price=625000&min_price=400000&search_distance_type=mi&srchType=T'
r = requests.get(url)

xml = xmltodict.parse(r.text)

items = xml['rdf:RDF']['item']
cols = None
rows = []
for item in items:
    row = {}
    if cols == None:
        cols = item.keys()
        if 'enc:enclosure' in cols:
            cols.remove('enc:enclosure')
    for col in cols:
        row[col] = item[col]
    if 'enc:enclosure' in item:
        row['img'] = item['enc:enclosure']['@resource']
    rows.append(row)

df = pd.DataFrame(rows)
df['dc:date'] = df['dc:date'].apply(pd.to_datetime)
otherRightsCount = df['dc:rights'].apply(lambda x: x !='&copy; 2016 <span class="desktop">craigslist</span><span class="mobile">CL</span>')
if otherRightsCount.sum():
    print 'Got a new result never seen before, investigate here', otherRightsCount
df.drop('dc:rights', axis=1, inplace=True)
df.drop('@rdf:about', axis=1, inplace=True)
df.drop('dc:type', axis=1, inplace=True)
df.drop('dcterms:issued', axis=1, inplace=True)
df.drop('title', axis=1, inplace=True)
ncols = []
for col in df.columns:
    if col.find('dc:') == 0:
        col = col[3:]
    ncols.append(col)
df.columns = ncols

uids = []
for r in range(df.shape[0]):
    row = df.iloc[r]
    ukeys = ['title', 'description', 'src']
    uid = md5.new(str(row[ukeys].tolist())).digest()
    uids.append(str(uid))
df['uid'] = uids
df.fillna('', inplace=True)


"""
create table craigslist_listing (
    listing_id bigint not null auto_increment
  , date datetime not null
  , language varchar(16) not null
  , source varchar(1024) not null
  , title varchar(1024) not null
  , description text
  , img varchar(1024)
  , link varchar(1024) not null
  , uid binary(16) not null
  , ts timestamp
  , primary key(listing_id)
);
"""
iquery = "INSERT INTO craigslist_listing (date, language, source, title, description, img, link, uid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"  
uquery = "UPDATE craigslist_listing SET date=%s, language=%s, source=%s, title=%s, description=%s, img=%s, link=%s WHERE BINARY uid = BINARY %s"

conn = MySQLdb.connect(host=dbhost, user=dbuser, passwd=dbpassword, db=dbname, charset='utf8')
cur = conn.cursor()
for r in range(df.shape[0]):
    try:
        row = df.iloc[r]
        z = cur.execute(uquery, (row['date'], row['language'], row['source'], row['title'], row['description'], row['img'], row['link'], row['uid'],))
        rowsaffected = cur.rowcount
        rowsaffected = int(re.search(r'Rows matched: (\d+)', cur._info).group(1))
        if rowsaffected == 0:
            print 'new', row['uid']
            cur.execute(iquery, (row['date'], row['language'], row['source'], row['title'], row['description'], row['img'], row['link'], row['uid'],))
        conn.commit()
    except:
        print('Could not insert', row['uid'], row['title'])
        conn.rollback()
conn.close()
