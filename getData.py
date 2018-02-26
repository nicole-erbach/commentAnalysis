import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import locale
import datetime
from difflib import SequenceMatcher


# return connection to database, create tables if not existent
def connectToOrCreateDatabase(databaseFilename):
    
    connection = sqlite3.connect(databaseFilename)
    cursor = connection.cursor()    
    
    # create tables if not existent
    sqlCommand = """
    CREATE TABLE IF NOT EXISTS articles (
    articleId INTEGER PRIMARY KEY,
    date TEXT,
    title TEXT,
    teaser TEXT)
    """
    cursor.execute(sqlCommand)
    
    sql_command = """
    CREATE TABLE IF NOT EXISTS users (
    userId INTEGER PRIMARY KEY,
    name TEXT)
    """
    cursor.execute(sqlCommand)
    
    sqlCommand = """
    CREATE TABLE IF NOT EXISTS comments (
    id INTEGER PRIMARY KEY,
    idOnTagesschau INTEGER,
    date TEXT,
    articleId INTEGER,
    userId INTEGER,
    title TEXT,
    text TEXT)
    """
    cursor.execute(sqlCommand)

    sqlCommand = """
    CREATE TABLE IF NOT EXISTS tags (
    articleId INTEGER,
    tag TEXT,
    UNIQUE (articleId, tag) ON CONFLICT IGNORE)
    """
    cursor.execute(sqlCommand)

    sqlCommand = """
    CREATE TABLE IF NOT EXISTS geotags (
    articleId INTEGER,
    geotag TEXT,
    UNIQUE (articleId, geotag) ON CONFLICT IGNORE)
    """
    cursor.execute(sqlCommand)

    sqlCommandCreate = """ 
    CREATE TABLE IF NOT EXISTS citations (
    id INTEGER PRIMARY KEY,
    originCommentId INTEGER,
    citationOccurrenceId INTEGER,
    citationStart INTEGER,
    citationLength INTEGER,
    UNIQUE (originCommentId, citationOccurrenceId,citationStart) ON CONFLICT IGNORE)
    """    
    cursor.execute(sqlCommand)
 
    connection.commit()
    cursor.close()
    
    return connection


# return a list of articleIds to visit based on last crawl and newest article on tagesschau.de
def getVisitListToCrawl(connection):

    maxCommentTime = datetime.timedelta(days=1)
    cursor = connection.cursor()

    # get newest article in database and go back 1 day to get ID of first article to visit 
    # (set to 118800 if database is empty)
    sqlCommand = """
    SELECT MIN(articleId) 
    FROM articles 
    WHERE date > datetime((SELECT MAX(date) FROM articles), '-1 day')
    """
    startId = cursor.execute(sqlCommand).fetchone()[0]

    if startId is None:
        startId = 118800

    # get id of newest article on tagesschau.de
    url = 'https://meta.tagesschau.de'
    response = requests.get(url)
    if not response.ok:
        print('meta.tagesschau unavailable. Try again later.')
        return []
    #soup = BeautifulSoup(response.text, 'html.parser')
    #idTitle = soup.find_all('div', 'box viewA')[0].find('a').attrs['href']
    #pattern = """\/id\/(\d+)\/(.*)"""
    #[newestId, title] = re.findall(pattern, response.text)[0]
    pattern = """\/id\/(\d+)\/.*"""
    newestId = re.findall(pattern, response.text)[0]
    
    # build ID-list of articles to crawl
    idList = range(startId, int(newestId))

    cursor.close()
    return idList


# get information from article (article metadata, teaser, comments) and write to database
def visitArticleToCrawl(connection, articleId):

    cursor = connection.cursor()

    url = 'https://meta.tagesschau.de/node/' + str(articleId)
    response = requests.get(url)
    if not response.ok:
        print('error parsing article ' + str(articleId) + '! Errorcode: ' + str(response.status_code))
        return
    
    soup = BeautifulSoup(response.text, 'html.parser')
     
    # extract article data
    articleTitle = soup.find('span', 'headline').get_text().strip()
   
    if articleTitle == '':
        print('article no ' + str(articleId) + ' is empty. Skipping.')
        return
 
    articleDateRaw = soup.find('h3', 'metaDate').get_text().strip()
    articleDate = datetime.datetime.strptime(articleDateRaw, '%d. %B %Y - %H:%M Uhr')
    
    teaserRaw = soup.find('span', 'teasertext').get_text()
    teaser = re.sub('\s+', ' ', teaserRaw).strip()[:-28] # crop 'Artikel auf tagesschau.de'
    
    # write article data to database (if not yet there)
    sql_command = "INSERT OR IGNORE INTO articles(articleId, date, title, teaser) VALUES(?,?,?,?)"
    cursor.execute(sql_command, [articleId, articleDate.isoformat(), articleTitle, teaser])
    
    # get tags and geotags and write to database (ignored if already there)
    bothTags = soup.find_all('div', 'taxonomy')
    if len(bothTags) > 0:
        tagList = bothTags[0].find_all('a')
        for tag in tagList:
            cursor.execute("INSERT OR IGNORE INTO tags(articleId, tag) VALUES(?,?)", [articleId, tag.get_text()])
    if len(bothTags) > 1:
        geotagList = bothTags[1].find_all('a')
        for geotag in geotagList:
            cursor.execute("INSERT OR IGNORE INTO geotags(articleId, geotag) VALUES(?,?)", [articleId, geotag.get_text()])
    connection.commit()    
        
    # extract data from comments
    newestCommentIdInDb = cursor.execute("SELECT MAX(id) FROM comments WHERE articleId=?",[articleId]).fetchone()[0]
    if newestCommentIdInDb is None:
        newestCommentIdInDb = 0
    
    allCommentsDivs = soup.find_all('div', 'comment')

    submittedPattern = 'Am (.*:\d{2}) von (.*)'
    for commentDiv in allCommentsDivs:
        
        commentId = int(commentDiv.find('a').attrs['id'][8:])
        if commentId <= newestCommentIdInDb:
            continue

        submittedString = commentDiv.find('div', 'submitted').get_text().strip()
        [commentDateRaw, commentAuthor] = re.findall(submittedPattern, submittedString)[0]
        commentDate = datetime.datetime.strptime(commentDateRaw, '%d. %B %Y um %H:%M')
        
        # get userId from database, evtl add new user before
        userId = cursor.execute("SELECT userId FROM users WHERE name=?", [commentAuthor]).fetchone()
        if userId is None:
            cursor.execute("INSERT INTO users(name) VALUES(?)", [commentAuthor])
            connection.commit()
            userId = cursor.execute("SELECT userId FROM users WHERE name=?", [commentAuthor]).fetchone()
        userId = userId[0]
            
        commentTitle = commentDiv.find('h3').get_text().strip()
        
        commentTextRaw = commentDiv.find('p').get_text()
        commentText = re.sub('\s+', ' ', commentTextRaw).strip()
        
        cursor.execute("INSERT INTO comments(idOnTagesschau, date, articleId, userId, title, text) VALUES(?,?,?,?,?,?)", [commentId, commentDate.isoformat(), articleId, userId, commentTitle, commentText])
    
    connection.commit()
    cursor.close()


# get list of articles to look for citations
def getVisitListToFindCitations(connection):

    cursor = connection.cursor()

    # get commentId of latest citation
    latestCitationId = cursor.execute("SELECT MAX(citationOccurrenceId) FROM citations").fetchone()[0]
   
    if latestCitationId == None:
        latestCitationId = 0
 
    # get list of articleIds with comments later than latest citation
    visitList = cursor.execute("SELECT articleId FROM comments WHERE id > ? GROUP BY articleId", [latestCitationId]).fetchall()

    cursor.close()
   
    # tuple -> integer
    visitList = [i[0] for i in visitList]
    return visitList


# compare all article-comments to identify citations and references and write to database (table citations)
# requirements for citation:  
    # - long common substring in quotas OR
    # - long common substring with reference authorname or post-time OR
    # - really long common substring

# requirements for reference
    # reference authorname and time OR
    # reference authorname and @ OR
    # reference time and @
def visitArticleToFindCitations(connection, articleId):

    cursor = connection.cursor()

    sqlCommandRead = """
        SELECT id, date, title, text, name 
        FROM comments NATURAL JOIN users GROUP BY id 
        HAVING articleId = ? 
        ORDER BY id
        """
    
    sqlCommandWrite = """
        INSERT OR IGNORE INTO citations(
        originCommentId, citationOccurrenceId, citationStart, citationLength) 
        VALUES(?,?,?,?)
        """
 
    # get all comments for this article
    comments = cursor.execute(sqlCommandRead, [articleId]).fetchall()
    
    # merge title and text, (if title is not yet included in text)
    text = []
    for post in comments:
        if post[1] in post[3]:
            text += [post[3]]
        else:
            text += [post[2] + ' ' + post[3]]

    # compare each pair of comments 
    for i, post in enumerate(text):
        if len(post) < 5:
            continue
        for j in range(i):

            # get longest common substring (50 characters+)
            seqM = SequenceMatcher(None, post, text[j]).find_longest_match(0, len(post), 0, len(text[j]))
            acc = 0
            if seqM.size > 50:
                if seqM.a + seqM.size == len(post): continue 
                # check for quotas,
                # occurrence of authorname
                # occurrence of time of origin post
                # or really long common substring
                if (post[seqM.a-1] == '\"' and post[seqM.a + seqM.size] == '\"' or 
                        comments[j][-1] in post or 
                        comments[j][1][-5:] in post or 
                        seqM.size > 100):
       
                    cursor.execute(sqlCommandWrite, [comments[j][0], comments[i][0], seqM.a, seqM.size])

            else:
                acc = 0
                
                # check for occurrence of authornames of earlier posts
                if comments[j][-1] in post:
                    acc += 1
            
                # check for occurrence of @authornames of earlier posts
                if "@" + comments[j][-1] in post:
                    acc += 1
            
                # check for occurrence of @ authornames of earlier posts
                if "@ " + comments[j][-1] in post:
                    acc += 1
            
                # check for occurrence of time of earlier posts
                if comments[j][1][-5:] in post:
                    acc += 1
            
            if acc > 1:
                cursor.execute(sqlCommandWrite, [comments[j][0], comments[i][0], 0, 0])

    connection.commit() 
    cursor.close()

def main():
    
    print('start getting data')
    
    # set locale to correctly parse date (monthname in german)
    locale.setlocale(locale.LC_ALL, 'de_DE.utf8')
    
    # connect to database
    databaseFilename = 'commentData.db'
    connection = connectToOrCreateDatabase(databaseFilename)
    
    # get all (new) articles and comments
    toVisitListCrawl = getVisitListToCrawl(connection)
    last = toVisitListCrawl[-1]
    for articleId in toVisitListCrawl:
        print(str(articleId) + ' / ' + str(last))
        visitArticleToCrawl(connection, articleId)
    
    print('got all data, find citations in comments')
    
    # analyse comments for citations and references
    toVisitListCitations = getVisitListToFindCitations(connection)
    last = toVisitListCitations[-1]
    for articleId in toVisitListCitations:
        print(str(articleId) + ' / ' + str(last))
        visitArticleToFindCitations(connection, articleId)
  
    connection.close()
    print('done')


if __name__ == "__main__": main()
