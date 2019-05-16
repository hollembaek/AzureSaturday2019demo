#%% Change working directory from the workspace root to the ipynb file location. Turn this addition off with the DataScience.changeDirOnImportExport setting
import os
try:
	os.chdir(os.path.join(os.getcwd(), 'databricks1'))
	print(os.getcwd())
except:
	pass

#%%
# from stripogram import HTML2Text
from bs4 import BeautifulSoup
import requests
import os

# libs for storage account
# import os # import OS dependant functionality
import pandas as pd  # import data analysis library required
from azure.storage.blob import BlockBlobService


#%%
# Blob storage Settings configs
blob_account_name = ""  # fill in your blob account name
blob_account_key = ""  # fill in your blob account key
mycontainer = ""  # fill in the container name
myblobname = "TestData_semi.txt"  # fill in the blob name for use with file input - must be semicolon delimited!
mydatafile = "TestData_semi.txt"  # fill in the output file name
myoutputfile = "TestData_semi_out.csv" #output is Comma delimited

# cognitive service configs
subscription_key_cognitive_service = ""  # fill in your subscription key
assert subscription_key_cognitive_service
text_analytics_base_url = "https://westeurope.api.cognitive.microsoft.com/text/analytics/v2.0/"  # update it from panel settings
subscription_key_translation_service = "" # fill in your subscription key


#%%
dtype_dic = {'SourceText': str, 'EnglishText': str, 'Entities': str, 'KeyPhrases':str}

def readfile():
    blob_service = BlockBlobService(account_name=blob_account_name, account_key=blob_account_key)
    blob_service.get_blob_to_path(mycontainer, myblobname, mydatafile)
    mydata = pd.read_csv(mydatafile, header=0, sep=";",dtype=dtype_dic)
    return mydata


def read_localfile():
    dtype_dic = {'SourceText': str, 'EnglishText': str, 'Entities': str, 'KeyPhrases':str}
    return pd.read_csv(mydatafile, header=0, sep=";", dtype=dtype_dic)


#%%
# core logic goes here
def getWebsiteText(website_url):
    rsp = requests.get(website_url)
    # print(rsp.text)
    soup = BeautifulSoup(rsp.text)
    # print(" ".join(soup.strings))
    body = soup.find('body')  # .get_text()

    # remove script tags
    [x.extract() for x in body.findAll('script')]
    body_text = " ".join(body.strings)
    # print(body_text.strip())
    return body_text.strip()


# 1 Detect language
def get_language(body_text):
    language_api_url = text_analytics_base_url + "languages"
    #print(language_api_url)
    documents = {'documents': [{'id': '1', 'text': body_text}]}

    headers = {"Ocp-Apim-Subscription-Key": subscription_key_cognitive_service}
    response = requests.post(language_api_url, headers=headers, json=documents)
    languages = response.json()
    #print(languages)
    lang = languages['documents'][0]['detectedLanguages'][0]['iso6391Name']  # error prone
    #print(lang)
    return lang


# 3 Key Phrase Extraction
def get_keyphrases(body_text, lang):
    key_phrase_api_url = text_analytics_base_url + "keyPhrases"
    documents = {'documents': [
        {'id': '1', 'language': lang, 'text': body_text}
    ]}

    headers = {'Ocp-Apim-Subscription-Key': subscription_key_cognitive_service}
    response = requests.post(key_phrase_api_url, headers=headers, json=documents)
    key_phrases = response.json()
    if key_phrases and 'documents' in key_phrases and 'keyPhrases' in key_phrases['documents'][0]:
        #print(key_phrases['documents'][0]['keyPhrases'])
        return key_phrases['documents'][0]['keyPhrases']


# 4 Named Entity Recognition
def get_named_entities(body_text):
    entity_linking_api_url = text_analytics_base_url + "entities"
    documents = {'documents': [
        {'id': '1', 'text': body_text}
    ]}
    headers = {"Ocp-Apim-Subscription-Key": subscription_key_cognitive_service}
    response = requests.post(entity_linking_api_url, headers=headers, json=documents)
    entities = response.json()
    if entities and 'documents' in entities and 'entities' in entities['documents'][0]:
        #print(entities['documents'][0]['entities'])
        return entities['documents'][0]['entities']


# 2 Translation
def translate_text(body_text, lang_from, lang_to):
    api_url = "https://api.cognitive.microsofttranslator.com/translate?api-version=3.0&to={1}"         .format(text_analytics_base_url, lang_to)

    body = [{
        'text': body_text
    }]

    headers = {
        'Ocp-Apim-Subscription-Key': subscription_key_translation_service,
        'Content-type': 'application/json',
        # 'X-ClientTraceId': str(uuid.uuid4())
    }
    try:
        request = requests.post(api_url, headers=headers, json=body)
        response = request.json()

        if len(response) > 0:
            response = response[0]

        if response and 'translations' in response and 'text' in response['translations'][0]:
            #print(response['translations'][0]['text'])
            return response['translations'][0]['text']
    except Exception as ex:
        print(ex)


#%%
def write_output_file(df_data, file_name):
  df_data.to_csv(os.path.join(os.getcwd(),file_name), encoding='utf-8', index=False)
  localfileprocessed = os.path.join(os.getcwd(),file_name) #assuming file is in current working directory
  #print(localfileprocessed)
  blob_service = BlockBlobService(account_name=blob_account_name, account_key=blob_account_key)

  try:
   #perform upload
   blob_service.create_blob_from_path(mycontainer,file_name,localfileprocessed)
  except Exception as ex:	        
       print (ex)


#%%
def run_pipeline():
  data = readfile()  # readfile()
  for index, row in data.iterrows():
      try:

          website_url = row['website']# test url "http://praiaazul.com/"  # row['website']
	  http = "http"
	  # original samples start with HTTP, but some of scraped addresses were missing it- this is a quick fix
          if website_url[:4] != http:
            website_url = "http://" + website_url
          print(website_url)
          # 1. browse the website and store the native text
          body_text = getWebsiteText(website_url)
          #print(body_text)
          if body_text:
              data.at[index, 'SourceText'] = body_text

          # check language and store translated text if language is different than english
          lang = get_language(body_text)
          if 'en' not in lang:
              translated = translate_text(body_text, lang, 'en')
              #print(translated)
              data.at[index, 'EnglishText'] = translated
              body_text = translated #use the translated text for the rest of the operations

          # Get the keyphrases and store them
          keyphrases = get_keyphrases(body_text, lang)
          if keyphrases:
              data.at[index, 'KeyPhrases'] = ", ".join(keyphrases)

          # get the named entites and update the file
          named_entites = get_named_entities(body_text)
          if named_entites:
              entity_name_only= []
              for entity in named_entites:
                  entity_name_only.append(entity['name'])
              data.at[index, 'Entities'] = ", ".join(entity_name_only)

      except Exception as ex:
          print(ex)

  # write them back to a file
  write_output_file(data, myoutputfile)


#%%
try:
  run_pipeline()
  print('Finished')
except Exception as ex:
  print(ex)


