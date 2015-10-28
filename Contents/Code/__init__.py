import re, time, unicodedata, hashlib, types
from collections import defaultdict

TVDB_API_KEY = 'D4DDDAEFAD083E6F'

# Plex Metadata endpoints
META_TVDB_GUID_SEARCH = 'http://meta.plex.tv/tv/guid/'
META_TVDB_QUICK_SEARCH = 'http://meta.plex.tv/tv/names/'
META_TVDB_TITLE_SEARCH = 'http://meta.plex.tv/tv/titles/'

# TVDB V2 API
TVDB_BASE_URL = 'https://thetvdb.com'
TVDB_V2_PROXY_SITE = 'https://tvdb2.plex.tv'
TVDB_LOGIN_URL = '%s/login' % TVDB_V2_PROXY_SITE
TVDB_SEARCH_URL = '%s/search/series?name=%%s' % TVDB_V2_PROXY_SITE
TVDB_SERIES_URL = '%s/series/%%s' % TVDB_V2_PROXY_SITE
TVDB_ACTORS_URL = '%s/actors' % TVDB_SERIES_URL
TVDB_SERIES_IMG_INFO_URL = '%s/images' % TVDB_SERIES_URL
TVDB_SERIES_IMG_QUERY_URL = '%s/query?keyType=%%s' % TVDB_SERIES_IMG_INFO_URL
TVDB_EPISODES_URL = '%s/episodes?page=%%s' % TVDB_SERIES_URL
TVDB_EPISODE_DETAILS_URL = '%s/episodes/%%s' % TVDB_V2_PROXY_SITE
TVDB_IMG_ROOT = '%s/banners/%%s' % TVDB_BASE_URL

GOOGLE_JSON_TVDB = 'http://ajax.googleapis.com/ajax/services/search/web?v=1.0&rsz=large&q=%s+"thetvdb.com"+series+%s'
GOOGLE_JSON_TVDB_TITLE = 'http://ajax.googleapis.com/ajax/services/search/web?v=1.0&rsz=large&q=%s+"thetvdb.com"+series+info+%s'
GOOGLE_JSON_BROAD = 'http://ajax.googleapis.com/ajax/services/search/web?v=1.0&rsz=large&q=%s+site:thetvdb.com+%s'
GOOGLE_JSON_IMDB = 'http://ajax.googleapis.com/ajax/services/search/web?v=1.0&rsz=large&q=%s+site:imdb.com+tv+%s'

# TMDb - themoviedb.org
TMDB_BASE_URL = 'https://api.tmdb.org/3'
TMDB_API_KEY = 'a3dc111e66105f6387e99393813ae4d5'
TMDB_CONFIG = '%s/configuration?api_key=%s' % (TMDB_BASE_URL, TMDB_API_KEY)

TMDB_TV_SEARCH = ('{base}/find/{{tvdb_id}}?external_source=tvdb_id&'
    'api_key={api_key}').format(base=TMDB_BASE_URL, api_key=TMDB_API_KEY)
TMDB_TV_SERIES_URL = ('{base}/tv/{{tmdb_id}}?'
    'api_key={api_key}').format(base=TMDB_BASE_URL, api_key=TMDB_API_KEY)
TMDB_TV_EPISODE_URL = ('{base}/tv/{{tmdb_id}}'
    '/season/{{season}}/episode/{{episode}}?'
    'api_key={api_key}').format(base=TMDB_BASE_URL, api_key=TMDB_API_KEY)
TMDB_TV_EPISODE_IMAGES_URL = ('{base}/tv/{{tmdb_id}}'
    '/season/{{season}}/episode/{{episode}}/images?'
    'api_key={api_key}').format(base=TMDB_BASE_URL, api_key=TMDB_API_KEY)

SCRUB_FROM_TITLE_SEARCH_KEYWORDS = ['uk','us']
NETWORK_IN_TITLE = ['bbc']
EXTRACT_AS_KEYWORDS = ['uk','us','bbc']

# Language table
# NOTE: if you add something here, make sure
# to add the language to the appropriate
# tvdb cache download script on the data
# processing servers
THETVDB_LANGUAGES_CODE = {
  'cs': '28',
  'da': '10',
  'de': '14',
  'el': '20',
  'en': '7',
  'es': '16',
  'fi': '11',
  'fr': '17',
  'he': '24',
  'hr': '31',
  'hu': '19',
  'it': '15',
  'ja': '25',
  'ko': '32',
  'nl': '13',
  'no': '9',
  'pl': '18',
  'pt': '26',
  'ru': '22',
  'sv': '8',
  'tr': '21',
  'zh': '27',
  'sl': '30'
}

GOOD_MATCH_THRESHOLD = 98  # Short circuit once we find a match better than this.

HEADERS = {'User-agent': 'Plex/Nine'}


def setJWT():

  try:
    jwtResp = JSON.ObjectFromString(HTTP.Request(TVDB_LOGIN_URL, data=JSON.StringFromObject(dict(apikey=TVDB_API_KEY)), headers={'Content-type': 'application/json'}).content)
  except Exception, e:
    Log("JWT Error: (%s) - %s" % (e, e.message))
    return

  if 'token' in jwtResp:
    HEADERS['Authorization'] = 'Bearer %s' % jwtResp['token']


def GetResultFromNetwork(url, fetchContent=True, additionalHeaders=None, data=None):

    if additionalHeaders is None:
      additionalHeaders = dict()

    Log("Retrieving URL: " + url)

    # Grab New Auth token
    if 'Authorization' not in HEADERS:
      setJWT()

    local_headers = HEADERS.copy()
    local_headers.update(additionalHeaders)

    try:
      result = HTTP.Request(url, headers=local_headers, timeout=60, data=data)
    except Exception:
      try:
        setJWT()
        result = HTTP.Request(url, headers=local_headers, timeout=60, data=data)
      except:
        return None

    if fetchContent:
      try:
        result = result.content
      except Exception, e:
        Log('Content Error (%s) - %s' % (e, e.message))

    return result

tmdb_base_image_url = JSON.ObjectFromString(GetResultFromNetwork(
    TMDB_CONFIG))['images']['base_url']

def Start():
  HTTP.CacheTime = CACHE_1HOUR * 24


def get_rating(tvdb_data, tmdb_data):
  try:
    tvdb_rating = float(tvdb_data['siteRating'])
  except:
    tvdb_rating = None
  try:
    tmdb_rating = float(tmdb_data['vote_average'])
  except:
    tmdb_rating = None
  if (Prefs['ratings'] == "Average of All Available Scores") and (tvdb_rating and tmdb_rating):
    return (tvdb_rating + tmdb_rating) / 2
  elif not (Prefs['ratings'] == "The Movie Database") and tvdb_rating:
    return tvdb_rating
  else:
    return tmdb_rating



class TVDBAgent(Agent.TV_Shows):

  name = 'TheTVDB'
  languages = [Locale.Language.English, 'fr', 'zh', 'sv', 'no', 'da', 'fi', 'nl', 'de', 'it', 'es', 'pl', 'hu', 'el', 'tr', 'ru', 'he', 'ja', 'pt', 'cs', 'ko', 'sl', 'hr']

  def getGoogleResult(self, url):
    res = JSON.ObjectFromURL(url)
    if res['responseStatus'] != 200:
      res = JSON.ObjectFromURL(url, cacheTime=0)
    time.sleep(3)
    return res

  def dedupe(self, results):

    # make sure to keep the highest score for the id
    results.Sort('score', descending=True)

    toWhack = []
    resultMap = {}
    for result in results:
      if not resultMap.has_key(result.id):
        resultMap[result.id] = True
      else:
        toWhack.append(result)
    for dupe in toWhack:
      results.Remove(dupe)

  def searchByGuid(self, results, lang, title, year):

    # Compute the GUID
    guid = self.titleyear_guid(title,year)

    penalty = 0
    maxPercentPenalty = 30
    maxLevPenalty = 10
    minPercentThreshold = 25

    try:
      res = XML.ElementFromURL(META_TVDB_GUID_SEARCH + guid[0:2] + '/' + guid + '.xml')
      for match in res.xpath('//match'):
        guid = match.get('guid')
        count = int(match.get('count'))
        pct = int(match.get('percentage'))
        penalty += int(maxPercentPenalty * ((100-pct)/100.0))

        Log('Inspecting: guid = %s, count = %s, pct = %s' % (guid, count, pct))

        if pct > minPercentThreshold:
          try:
            series_data = JSON.ObjectFromString(GetResultFromNetwork(TVDB_SERIES_URL % guid, additionalHeaders={'Accept-Language': lang}))['data']
            name = series_data['seriesName']

            if '403: series not permitted' in name.lower():
              continue

            penalty += int(maxLevPenalty * (1 - self.lev_ratio(name, title)))
            try: year = series_data['firstAired'].split('-')[0]
            except: year = None
            Log('Adding (based on guid lookup) id: %s, name: %s, year: %s, lang: %s, score: %s' % (match.get('guid'), name, year, lang, 100 - penalty))
            results.Append(MetadataSearchResult(id=str(match.get('guid')), name=name, year=year, lang=lang, score=100 - penalty))
          except:
            continue

    except Exception, e:
      Log(repr(e))
      pass

  def searchByWords(self, results, lang, origTitle, year):
    # Process the text.
    title = origTitle.lower()
    title = re.sub(r'[\'":\-&,.!~()]', ' ', title)
    title = re.sub(r'[ ]+', ' ', title)

    # Search for words.
    show_map = {}
    total_words = 0

    for word in title.split():
      if word not in ['a', 'the', 'of', 'and']:
        total_words += 1
        wordHash = hashlib.sha1()
        wordHash.update(word.encode('utf-8'))
        wordHash = wordHash.hexdigest()
        try:
          matches = XML.ElementFromURL(META_TVDB_QUICK_SEARCH + lang + '/' + wordHash[0:2] + '/' + wordHash + '.xml', cacheTime=60)
          for match in matches.xpath('//match'):
            tvdb_id = match.get('id')
            title = match.get('title')
            titleYear = match.get('year')
            # Make sure we use the None type (not the string 'None' which evaluates to true and sorts differently).
            if titleYear == 'None':
              titleYear = None

            if not show_map.has_key(tvdb_id):
              show_map[tvdb_id] = [tvdb_id, title, titleYear, 1]
            else:
              show_map[tvdb_id] = [tvdb_id, title, titleYear, show_map[tvdb_id][3] + 1]
        except:
          pass

    resultList = show_map.values()
    resultList.sort(lambda x, y: cmp(y[3],x[3]))

    for i, result in enumerate(resultList):

      if i > 10:
        break

      score = 90 # Start word matches off at a slight defecit compared to guid matches.
      theYear = result[2]

      # Remove year suffixes that can mess things up.
      searchTitle = origTitle
      if len(origTitle) > 8:
        searchTitle = re.sub(r'([ ]+\(?[0-9]{4}\)?)', '', searchTitle)

      foundTitle = result[1]
      if len(foundTitle) > 8:
        foundTitle = re.sub(r'([ ]+\(?[0-9]{4}\)?)', '', foundTitle)

      # Remove prefixes that can screw things up.
      searchTitle = re.sub('^[Bb][Bb][Cc] ', '', searchTitle)
      foundTitle = re.sub('^[Bb][Bb][Cc] ', '', foundTitle)

      # Adjust if both have 'the' prefix by adding a prefix that won't be stripped.
      distTitle = searchTitle
      distFoundTitle = foundTitle
      if searchTitle.lower()[0:4] == 'the ' and foundTitle.lower()[0:4] == 'the ':
        distTitle = 'xxx' + searchTitle
        distFoundTitle = 'xxx' + foundTitle

      # Score adjustment for title distance.
      score = score - int(30 * (1 - self.lev_ratio(searchTitle, foundTitle)))

      # Discount for mismatched years.
      if theYear is not None and year is not None and theYear != year:
        score = score - 5

      # Discout for later results.
      score = score - i * 5

      # Use a relatively high threshold here to avoid pounding TheTVDB with a bunch of bogus stuff that 404's on our proxies.
      if score > 70:

        # Make sure TheTVDB has heard of this show and we'll be able to parse the results.
        try:
          series_data = JSON.ObjectFromString(GetResultFromNetwork(TVDB_SERIES_URL % result[0], additionalHeaders={'Accept-Language': lang}))['data']
          Log('Adding (based on word matches) id: %s, name: %s, year: %s, lang: %s, score: %s' % (result[0],result[1],result[2],lang,score))
          results.Append(MetadataSearchResult(id=str(result[0]), name=result[1], year=result[2], lang=lang, score=score))
        except:
          Log('Skipping match with id %s: failed TVDB lookup.' % result[0])

    # Sort.
    results.Sort('score', descending=True)


  def search(self, results, media, lang, manual=False):

    if media.primary_agent == 'com.plexapp.agents.themoviedb':

      # Get the TVDB id from the Movie Database Agent
      tvdb_id = Core.messaging.call_external_function(
        'com.plexapp.agents.themoviedb',
        'MessageKit:GetTvdbId',
        kwargs = dict(
          tmdb_id = media.primary_metadata.id
        )
      )

      if tvdb_id:
        results.Append(MetadataSearchResult(
          id = str(tvdb_id),
          score = 100
        ))

      return

    doGoogleSearch = False

    # MAKE SURE WE USE precomposed form, since that seems to be what TVDB prefers.
    media.show = unicodedata.normalize('NFC', unicode(media.show)).strip()

    # If we got passed in something that looks like an ID, use it.
    if len(media.show) > 3 and re.match('^[0-9]+$', media.show) is not None:
      url = TVDB_BASE_URL + '?tab=series&id=' + media.show
      self.TVDBurlParse(media, lang, results, 100, 0, url)

    if not doGoogleSearch:

      # GUID-based matches.
      self.searchByGuid(results, lang, media.show, media.year)
      results.Sort('score', descending=True)

      for i,r in enumerate(results):
        if i > 2:
          break
        Log('Top GUID result: ' + str(results[i]))

      if not len(results) or results[0].score <= GOOD_MATCH_THRESHOLD:
        # No good-enough matches in GUID search, try word matches.
        self.searchByWords(results, lang, media.show, media.year)
        self.dedupe(results)
        results.Sort('score', descending=True)

        for i,r in enumerate(results):
          if i > 2:
            break
          Log('Top GUID+name result: ' + str(results[i]))

    if len(results) == 0 or manual:
      doGoogleSearch = True

    mediaYear = ''
    if media.year is not None:
      mediaYear = ' (' + media.year + ')'
    w = media.show.lower().split(' ')
    keywords = ''
    for k in EXTRACT_AS_KEYWORDS:
      if k.lower() in w:
        keywords = keywords + k + '+'
    cleanShow = self.util_cleanShow(media.show, SCRUB_FROM_TITLE_SEARCH_KEYWORDS)
    cs = cleanShow.split(' ')
    cleanShow = ''
    for x in cs:
      cleanShow = cleanShow + 'intitle:' + x + ' '

    cleanShow = cleanShow.strip()
    origShow = media.show
    SVmediaShowYear = {'normal':String.Quote((origShow + mediaYear).encode('utf-8'), usePlus=True).replace('intitle%3A', 'intitle:'),
                       'clean': String.Quote((cleanShow + mediaYear).encode('utf-8'), usePlus=True).replace('intitle%3A','intitle:')}
    mediaShowYear = SVmediaShowYear['normal']

    if doGoogleSearch:
      searchVariations = [SVmediaShowYear]
      if media.year is not None:
        SVmediaShow = {'normal':String.Quote(origShow.encode('utf-8'), usePlus=True).replace('intitle%3A', 'intitle:'),
                       'clean': String.Quote(cleanShow.encode('utf-8'), usePlus=True).replace('intitle%3A', 'intitle:')}
        searchVariations.append(SVmediaShow)

      #option to perform searches without the year, in the event we have no results over our match threshold
      for sv in searchVariations:
        #check to make sure we want to run these searches again WITHOUT the year hint, if there was one passed in
        if len(results) > 0:
          results.Sort('score', descending=True)
          if results[0].score >= 80:
            Log('skipping search engines')
            break #don't bother trying search without year, we have a match
        Log('hitting search engines')

        #run through several search engines
        resultDict = {}
        @parallelize
        def hitSearchEngines():
          for s in [GOOGLE_JSON_TVDB, GOOGLE_JSON_TVDB_TITLE, GOOGLE_JSON_IMDB, GOOGLE_JSON_BROAD]:
            resultDict[s] = []
            @task
            def UpdateEpisode(s=s, sv=sv):

              if s in [GOOGLE_JSON_TVDB_TITLE]:
                tmpMediaShowYear = sv['clean']
              else:
                tmpMediaShowYear = sv['normal']

              #make sure we have results and normalize
              if s.count('googleapis.com') > 0:

                try:
                  jsonObj = self.getGoogleResult(s % (tmpMediaShowYear, keywords))['responseData']['results']
                except AttributeError:
                  return

                if len(jsonObj) > 0:
                  for res in jsonObj:
                    scorePenalty = 0
                    res_url = None
                    if s.count('googleapis.com') > 0:
                      res_url = res['unescapedUrl']

                    if res_url:
                      resultDict[s].append((res_url, scorePenalty))

        @parallelize
        def loopResults():
          for s in resultDict:
            if s in [GOOGLE_JSON_TVDB, GOOGLE_JSON_IMDB, GOOGLE_JSON_TVDB_TITLE, GOOGLE_JSON_BROAD]:
              score = 99
            else:
              break
            for url, scorePenalty in resultDict[s]:
              @task
              def lookupResult(score=score, url=url, scorePenalty=scorePenalty):
                self.TVDBurlParse(media, lang, results, score, scorePenalty, url)
              score = score - 5

    #try an exact tvdb match
    try:
      Log('Searching for exact match with: ' + mediaShowYear)
      series_data = JSON.ObjectFromString(GetResultFromNetwork(TVDB_SEARCH_URL % mediaShowYear, additionalHeaders={'Accept-Language': lang}))['data'][0]
      series_name = series_data['seriesName']
      if series_name.lower().strip() == media.show.lower().strip():
        self.ParseSeries(media, series_data, lang, results, 90)
      elif series_name[:series_name.rfind('(')].lower().strip() == media.show.lower().strip():
        self.ParseSeries(media, series_data, lang, results, 86)
    except Exception, e:
      Log(repr(e))
      pass

    self.dedupe(results)

    #hunt for duplicate shows with different years
    resultMap = {}
    for result in results:
      for check in results:
        if result.name == check.name and result.id != check.id:
          resultMap[result.year] = result

    years = resultMap.keys()
    years.sort(reverse=True)

    #bump the score of newer dupes
    i=0
    for y in years[:-1]:
      if resultMap[y].score <= resultMap[years[i+1]].score:
        resultMap[y].score = resultMap[years[i+1]].score + 1

    for i,r in enumerate(results):
      if i > 10:
        break
      Log('Final result: ' + str(results[i]))

  def TVDBurlParse(self, media, lang, results, score, scorePenalty, url):
    if url.count('tab=series&id='):
      seriesRx = 'tab=series&id=([0-9]+)'
      m = re.search(seriesRx, url)
    elif url.count('tab=seasonall&id='):
      seriesRx = 'tab=seasonall&id=([0-9]+)'
      m = re.search(seriesRx, url)
    else:
      seriesRx = 'seriesid=([0-9]+)'
      m = re.search(seriesRx, url)
    if m:
      try:
        series_data = JSON.ObjectFromString(GetResultFromNetwork(TVDB_SERIES_URL % m.groups(1)[0], additionalHeaders={'Accept-Language': lang}))['data']
        if len(series_data):
          self.ParseSeries(media, series_data, lang, results, score - scorePenalty)
      except Exception, e:
        Log('Couldn\'t find Series in TVDB XML: ' + str(e))

  def ParseSeries(self, media, series_data, lang, results, score):

    # Get attributes from the JSON
    series_id = series_data.get('id', '')
    series_name = series_data.get('seriesName', '')
    series_lang = lang

    if series_name is '' or '403: series not permitted' in series_name.lower():
      return

    try:
      series_year = series_data['firstAired'][:4]
    except:
      series_year = None

    if not series_name:
      return

    if not media.year:
      clean_series_name = series_name.replace('(' + str(series_year) + ')','').strip().lower()
    else:
      clean_series_name = series_name.lower()

    cleanShow = self.util_cleanShow(media.show, NETWORK_IN_TITLE)

    substringLen = len(Util.LongestCommonSubstring(cleanShow.lower(), clean_series_name))
    cleanShowLen = len(cleanShow)

    maxSubstringPoints = 5.0  # use a float
    score += int((maxSubstringPoints * substringLen)/cleanShowLen)  # max 15 for best substring match

    distanceFactor = .6
    score = score - int(distanceFactor * Util.LevenshteinDistance(cleanShow.lower(), clean_series_name))

    if series_year and media.year:
      if media.year == series_year:
        score += 10
      else:
        score = score - 10

    # sanity check to make sure we have SOME common substring
    if (float(substringLen) / cleanShowLen) < .15:  # if we don't have at least 15% in common, then penalize below the 80 point threshold
      score = score - 25

    # Add a result for this show
    results.Append(
      MetadataSearchResult(
          id=str(series_id),
          name=series_name,
          year=series_year,
          lang=series_lang,
          score=score
      )
    )

  def update(self, metadata, media, lang):
    Log("def update()")

    tvdb_series_data = defaultdict(lambda: '')
    try:
      tvdb_series_data.update(JSON.ObjectFromString(
        GetResultFromNetwork(TVDB_SERIES_URL % metadata.id,
          additionalHeaders={'Accept-Language': lang})
        )['data']
      )
    except (TypeError, KeyError):
      Log("Bad series data, no update for TVDB id: %s" % metadata.id)
      # TODO: Add function to search TMDb by name
      return

    Log('tmdb tvdb_series_data: %r' % tvdb_series_data)

    try:
      tvdb_id = metadata.id
      tmdb_id = self.get_tmdb_id(tvdb_id)
      tmdb_series_data = defaultdict(lambda: '')
      tmdb_series_data.update(JSON.ObjectFromString(
        GetResultFromNetwork(TMDB_TV_SERIES_URL.format(tmdb_id=tmdb_id))
      ))
      Log('tmdb_series_url: %s' % TMDB_TV_SERIES_URL.format(tmdb_id=tmdb_id))
      Log('tmdb_series_data: %r' % tmdb_series_data)
    except Exception, e:
      Log('tmdb_series_data error %s' % e)

    actor_data = None
    try:
      actor_data = JSON.ObjectFromString(GetResultFromNetwork(TVDB_ACTORS_URL % metadata.id, additionalHeaders={'Accept-Language': lang}))['data']
    except KeyError:
      Log("Bad actor data, no update for TVDB id: %s" % metadata.id)

    metadata.title = tvdb_series_data['seriesName']
    metadata.content_rating = tvdb_series_data['rating']
    metadata.studio = tvdb_series_data['network']


    if Prefs['seriesplot'] == "The Movie Database":
      metadata.summary = (tmdb_series_data['overview']
                      or tvdb_series_data['overview'])
    else:
      metadata.summary = (tvdb_series_data['overview']
                      or tmdb_series_data['overview'])

    # Convenience Function
    parse_date = lambda s: Datetime.ParseDate(s).date()

    try:
      originally_available_at = tvdb_series_data['firstAired']
      if len(originally_available_at) > 0:
        metadata.originally_available_at = parse_date(originally_available_at)
      else:
        metadata.originally_available_at = None
    except: pass

    try:
      tvdb_runtime = int(tvdb_series_data['runtime'])
    except (ValueError, KeyError):
      tvdb_runtime = None
    try:
      tmdb_runtime = int(tmdb_series_data['episode_run_time'][0])
    except (ValueError, KeyError):
      tmdb_runtime = None
    metadata.duration = tvdb_runtime or tmdb_runtime

    metadata.rating = get_rating(tvdb_series_data, tmdb_series_data)
    metadata.genres = tvdb_series_data.get('genre', [])

    # Cast

    try:
      _ = (e for e in actor_data)  # test if actor data is iterable ie not null

      metadata.roles.clear()
      for actor in actor_data:
        try:
          role = metadata.roles.new()
          role.role = actor['role']
          role.actor = actor['name']
        except:
          pass
    except AttributeError:
      pass

    # Create List of episodes
    episode_data = []
    next_page = 1
    try:
      while isinstance(next_page, int) or (isinstance(next_page, basestring) and next_page.isdigit()):
        next_page = int(next_page)
        episode_data_page = JSON.ObjectFromString(GetResultFromNetwork(TVDB_EPISODES_URL % (metadata.id, next_page), additionalHeaders={'Accept-Language': lang}))
        episode_data.extend(episode_data_page['data'])
        next_page = episode_data_page['links']['next']
    except:
      pass

    # Get episode data
    @parallelize
    def UpdateEpisodes():

      for episode_info in episode_data:

        # Get the season and episode numbers
        season_num = str(episode_info.get('airedSeason', ''))
        episode_num = str(episode_info.get('airedEpisodeNumber', ''))

        if media is not None:
          # Also get the air date for date-based episodes.
          try:
            originally_available_at = parse_date(episode_info['firstAired'])
            date_based_season = originally_available_at.year
          except:
            originally_available_at = date_based_season = None

          if not ((season_num in media.seasons and episode_num in media.seasons[season_num].episodes) or
                  (originally_available_at is not None and date_based_season in media.seasons and originally_available_at in media.seasons[date_based_season].episodes) or
                  (originally_available_at is not None and season_num in media.seasons and originally_available_at in media.seasons[season_num].episodes)):
            #Log("No media for season %s episode %s - skipping population of episode data", season_num, episode_num)
            continue

        # Get the episode object from the model
        episode = metadata.seasons[season_num].episodes[episode_num]

        # Add tmdb_id to episode_info to allow UpdateEpisode to access it
        episode_info['tmdb_id'] = tmdb_id

        # Create a task for updating this episode
        @task
        def UpdateEpisode(episode=episode, episode_info=episode_info):

          episode_id = str(episode_info['id'])
          tvdb_episode_details = defaultdict(lambda: '',
              JSON.ObjectFromString(GetResultFromNetwork(
                TVDB_EPISODE_DETAILS_URL % episode_id,
                additionalHeaders={'Accept-Language': lang})
              )['data']
          )

          # Need to reassign these because of parallel tasks
          season_num = tvdb_episode_details['airedSeason']
          episode_num = tvdb_episode_details['airedEpisodeNumber']
          tmdb_id = episode_info['tmdb_id']

          tmdb_episode_details = defaultdict(lambda: '',
              JSON.ObjectFromString(GetResultFromNetwork(
                TMDB_TV_EPISODE_URL.format(
                  season = season_num,
                  episode = episode_num,
                  tmdb_id = tmdb_id
                ))
              )
          )

          lev =  self.lev_ratio(
              tvdb_episode_details['episodeName'],
              tmdb_episode_details['name']
          )
          if lev < 0.6:
            # if the titles of the episodes aren't the same,
            # clear the data from TMDb, and just use thetvdb
            tmdb_episode_details.clear()

          # Get episode information

          episode.title = tvdb_episode_details['episodeName']
          if Prefs['episodeplot'] == "The Movie Database":
            episode.summary = (tmdb_episode_details['overview']
                            or tvdb_episode_details['overview'])
          else:
            episode.summary = (tvdb_episode_details['overview']
                            or tmdb_episode_details['overview'])

          try: episode.absolute_number = int(tvdb_episode_details['absoluteNumber'])
          except: pass

          episode.rating = get_rating(tvdb_episode_details, tmdb_episode_details)

          try:
            originally_available_at = tvdb_episode_details['firstAired']
            if originally_available_at is not None and len(originally_available_at) > 0:
              episode.originally_available_at = parse_date(originally_available_at)
          except:
            pass

          episode.directors = [] if tvdb_episode_details.get('director') is None else [tvdb_episode_details.get('director')]
          episode.writers = tvdb_episode_details.get('writers', [])

          # Download the episode thumbnail
          valid_names = list()

          if tvdb_episode_details.get('filename'):
            thumb_file = tvdb_episode_details.get('filename')
            if thumb_file is not None and len(thumb_file) > 0:
              thumb_url = TVDB_IMG_ROOT % thumb_file
              thumb_data = GetResultFromNetwork(thumb_url, False)

              # Check that the thumb doesn't already exist before downloading it
              valid_names.append(thumb_url)
              if thumb_url not in episode.thumbs:
                try:
                  episode.thumbs[thumb_url] = Proxy.Media(thumb_data)
                except:
                  # tvdb doesn't have a thumb for this show
                  pass

          tmdb_episode_art = defaultdict(lambda: '',
              JSON.ObjectFromString(GetResultFromNetwork(
                TMDB_TV_EPISODE_IMAGES_URL.format(
                  season = season_num,
                  episode = episode_num,
                  tmdb_id = tmdb_id
                ))
              )
          )
          Log('tmdb_episode_art = %r' % tmdb_episode_art)

          for still in tmdb_episode_art['stills']:
            # TODO: change size??
            still_url = '%soriginal%s' % (tmdb_base_image_url, still['file_path'])
            valid_names.append(still_url)
            if still_url not in episode.thumbs:
              still_data = GetResultFromNetwork(still_url, False)
              episode.thumbs[still_url] = Proxy.Media(still_data)

          episode.thumbs.validate_keys(valid_names)

    # Maintain a list of valid image names
    valid_names = list()

    # Get Image Counts
    image_types = {}
    try:
      image_types = JSON.ObjectFromString(GetResultFromNetwork(TVDB_SERIES_IMG_INFO_URL % metadata.id, additionalHeaders={'Accept-Language': lang}))['data']
    except KeyError:
      Log("Bad image type data for TVDB id: %s" % metadata.id)

    img_list = []
    for image_type, num_imgs in image_types.iteritems():
      try:
        img_list.extend(JSON.ObjectFromString(GetResultFromNetwork(TVDB_SERIES_IMG_QUERY_URL % (metadata.id, image_type), additionalHeaders={'Accept-Language': lang}))['data'])
      except KeyError:
        Log("Bad image type query data for TVDB id: %s (image_type: %s)" % (metadata.id, image_type))

    @parallelize
    def DownloadImages():

      # Add a download task for each image
      i = 0
      for img_info in img_list:
        i += 1
        @task
        def DownloadImage(metadata=metadata, img_info=img_info, i=i, valid_names=valid_names):

          # Parse the banner.
          banner_type, banner_path, banner_thumb, proxy = self.parse_banner(img_info)

          # Compute the banner name and prepare the data
          banner_name = TVDB_IMG_ROOT % banner_path
          banner_url = TVDB_IMG_ROOT % banner_thumb

          valid_names.append(banner_name)

          # Find the attribute to add to based on the image type, checking that data doesn't
          # already exist before downloading
          if banner_type == 'fanart' and banner_name not in metadata.art:
            try: metadata.art[banner_name] = proxy(self.banner_data(banner_url), sort_order=i)
            except Exception, e: Log(str(e))

          elif banner_type == 'poster' and banner_name not in metadata.posters:
            try: metadata.posters[banner_name] = proxy(self.banner_data(banner_url), sort_order=i)
            except Exception, e: Log(str(e))

          elif banner_type == 'series' and banner_name not in metadata.banners:
            try: metadata.banners[banner_name] = proxy(self.banner_data(banner_url), sort_order=i)
            except Exception, e: Log(str(e))

          elif banner_type in ['season', 'seasonwide']:
            season_num = str(img_info.get('subKey', ''))

            # Need to check for date-based season (year) as well.
            try: date_based_season = str(int(season_num) + metadata.originally_available_at.year - 1)
            except: date_based_season = None

            if media is None or season_num in media.seasons or date_based_season in media.seasons:
              if banner_name not in metadata.seasons[season_num].posters:
                try: metadata.seasons[season_num].posters[banner_name] = proxy(self.banner_data(banner_url), sort_order=i)
                except Exception, e: Log(str(e))

    # Fallback to foreign art if localized art doesn't exist.
    if len(metadata.art) == 0 and lang == 'en':
      i = 0
      for img_info in img_list:
        banner_type, banner_path, banner_lang, banner_thumb, proxy = self.parse_banner(img_info)
        banner_name = TVDB_IMG_ROOT % banner_path
        if banner_type == 'fanart' and banner_name not in metadata.art:
          try: metadata.art[banner_name] = proxy(self.banner_data(TVDB_IMG_ROOT % banner_thumb), sort_order=i)
          except: pass

    # Check each poster, background & banner image we currently have saved. If any of the names are no longer valid, remove the image
    metadata.posters.validate_keys(valid_names)
    metadata.art.validate_keys(valid_names)
    metadata.banners.validate_keys(valid_names)

  def parse_banner(self, img_info):

    # Get the image attributes from the XML
    banner_type = img_info.get('keyType', '')
    banner_path = img_info.get('fileName', '')
    banner_thumb = '_cache/%s' % banner_path
    proxy = Proxy.Preview

    return banner_type, banner_path, banner_thumb, proxy

  def banner_data(self, path):
    return GetResultFromNetwork(path, False)

  def util_cleanShow(self, cleanShow, scrubList):
    for word in scrubList:
      c = word.lower()
      l = cleanShow.lower().find('(' + c + ')')
      if l >= 0:
        cleanShow = cleanShow[:l] + cleanShow[l+len(c)+2:]
      l = cleanShow.lower().find(' ' + c)
      if l >= 0:
        cleanShow = cleanShow[:l] + cleanShow[l+len(c)+1:]
      l = cleanShow.lower().find(c + ' ')
      if l >= 0:
        cleanShow = cleanShow[:l] + cleanShow[l+len(c)+1:]
    return cleanShow

  def identifierize(self, string):
    string = re.sub( r"\s+", " ", string.strip())
    string = unicodedata.normalize('NFKD', self.safe_unicode(string))
    string = re.sub(r"['\"!?@#$&%^*\(\)_+\.,;:/]","", string)
    string = re.sub(r"[_ ]+","_", string)
    string = string.strip('_')
    return string.strip().lower()

  def safe_unicode(self, s,encoding='utf-8'):
    if s is None:
      return None
    if isinstance(s, basestring):
      if isinstance(s, types.UnicodeType):
        return s
      else:
        return s.decode(encoding)
    else:
      return str(s).decode(encoding)

  def guidize(self,string):
    hash = hashlib.sha1()
    hash.update(string.encode('utf-8'))
    return hash.hexdigest()

  def titleyear_guid(self, title, year=None):
    if title is None:
      title = ''

    if year == '' or year is None or not year:
      string = u"%s" % self.identifierize(title)
    else:
      string = u"%s_%s" % (self.identifierize(title), year)
    return self.guidize(string)

  def lev_ratio(self,s1,s2):
    distance = Util.LevenshteinDistance(self.safe_unicode(s1),self.safe_unicode(s2))
    max_len = float(max([ len(s1), len(s2) ]))

    ratio = 0.0
    try:
      ratio = float(1 - (distance/max_len))
    except:
      pass

    return ratio

  def best_title_by_language(self, lang, localTitle, tvdbID ):

    # this returns not only the best title, but the best
    # levenshtien ratio found amongst all of the titles
    # in the title list... the lev ratio is to give an overall
    # confidence that the local title corresponds to the
    # tvdb id.. even if the picked title is in a language
    # other than the locally named title

    titles = {'best_lev_ratio': {'title': None, 'lev_ratio': -1.0}}   # -1 to force > check later
    try:
      res = XML.ElementFromURL(META_TVDB_TITLE_SEARCH + tvdbID[0:2] + '/' + tvdbID + '.xml')
      for row in res.xpath("/records/record"):
        t = row['title']
        l = row['lang']
        lev = self.lev_ratio(localTitle,t)
        titles[lang] = {'title': t, 'lev_ratio': lev, 'lang': l}
        if lev > titles.get('best_lev_ratio').get('lev_ratio'):
          titles['best_lev_ratio'] = {'title': t, 'lev_ratio': lev, 'lang': l}
    except Exception, e:
      Log(e)
      return localTitle, lang, 0.0

    bestLevRatio = titles.get('best_lev_ratio').get('lev_ratio')
    if bestLevRatio < 0:
      return localTitle, lang, 0.0

    if titles.has_key(lang):
      useTitle = titles.get(lang)
    elif titles.has_key('en'):
      useTitle = titles.get('en')
    else:
      useTitle = titles.get('best_lev_ratio')

    return useTitle.get('title'), useTitle.get('lang'), useTitle.get('lev_ratio')

  def get_tmdb_id(self, tvdb_id):
    """Return the id for themoviedb.org, """
    url = TMDB_TV_SEARCH.format(tvdb_id=tvdb_id)
    results = JSON.ObjectFromString(GetResultFromNetwork(url))
    Log('Search for tmdb_id with url {url}'.format(url=url))
    if len(results['tv_results']) == 1:
      tmdb_id = results['tv_results'][0]['id']
      Log('TVDB id %s matches TMDb id %s' % (tvdb_id, tmdb_id))
      return tmdb_id
    else:
      Log('Unable to get unique TMDb match for TVDB id %s' % tvdb_id)
