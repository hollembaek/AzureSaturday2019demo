# from stripogram import HTML2Text
from bs4 import BeautifulSoup
import requests

# libs for storage account
# import os # import OS dependant functionality
import pandas as pd  # import data analysis library required
from azure.storage.blob import BlockBlobService

# Blob storage Settings configs
blob_account_name = "wsdccontainersrgdiag839"  # fill in your blob account name
blob_account_key = "4MZzcceV6leI8FPDFaqXZOSrm9W0mbyW1DfYY7Hx/gd3dKNPh5+uMRwVUDChD0OwS0RpCuChb8xnmS+F/3xgog=="  # fill in your blob account key
mycontainer = "davidtask"  # fill in the container name
myblobname = "TestData_semi.txt"  # fill in the blob name
mydatafile = "TestData_semi.txt"  # fill in the output file name

# cognitive service configs
subscription_key_cognitive_service = "6f154a5f8a7e47f180d62e00ec5f0164"  #
assert subscription_key_cognitive_service
text_analytics_base_url = "https://westeurope.api.cognitive.microsoft.com/text/analytics/v2.0/"  # update it from panel settings
subscription_key_translation_service = "a6ab550405c34669b4dfe475030159a1"


def readfile():
    # containers = blob_service.list_containers()
    # for c in containers:
    #  print(c.name)
    # print(blob_service.list_containers()[0])

    # blobs
    # blobs = blob_service.list_blobs('davidtask')
    # for b in blobs:
    #  print(b.name)
    # blob_service.get_blob_to_path('azure-notebooks-data', 'sample.txt', mydatafile)
    # os.remove(os.path.join(dirname, mydatafile))

    blob_service = BlockBlobService(account_name=blob_account_name, account_key=blob_account_key)
    blob_service.get_blob_to_path(mycontainer, myblobname, mydatafile)
    mydata = pd.read_csv(mydatafile, header=0, sep=";")

    # print(mydata.head())
    return mydata


def read_localfile():
    dtype_dic = {'SourceText': str, 'EnglishText': str, 'Entities': str, 'KeyPhrases':str}
    return pd.read_csv(mydatafile, header=0, sep=";", dtype=dtype_dic)


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
    print(language_api_url)
    documents = {'documents': [{'id': '1', 'text': body_text}]}

    headers = {"Ocp-Apim-Subscription-Key": subscription_key_cognitive_service}
    response = requests.post(language_api_url, headers=headers, json=documents)
    languages = response.json()
    print(languages)
    lang = languages['documents'][0]['detectedLanguages'][0]['iso6391Name']  # error prone
    print(lang)
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
        print(key_phrases['documents'][0]['keyPhrases'])
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
        print(entities['documents'][0]['entities'])
        return entities['documents'][0]['entities']


# 2 Translation
def translate_text(body_text, lang_from, lang_to):
    api_url = "https://api.cognitive.microsofttranslator.com/translate?api-version=3.0&to={1}" \
        .format(text_analytics_base_url, lang_to)

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
            print(response['translations'][0]['text'])
            return response['translations'][0]['text']
    except Exception as ex:
        print(ex)


def run_pipeline():
    data = read_localfile()  # readfile()
    for index, row in data.iterrows():
        try:
            website_url = "http://praiaazul.com/"  # row['website']

            # 1. browse the website and store the native text
            body_text = getWebsiteText(website_url)

            if body_text:
                data.at[index, 'SourceText'] = body_text

            # check language and store translated text if language is different than english
            lang = get_language(body_text)
            if 'en' not in lang:
                translated = translate_text(body_text, lang, 'en')
                print(translated)
                data.at[index, 'EnglishText'] = translated

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
    data.to_csv(mydatafile)


def test_method():
    print('starting')

    rsp = requests.get('https://www.google.com')
    soup = BeautifulSoup(rsp.text)

    body = soup.find('body')  # .get_text()
    # remove script tags
    [x.extract() for x in body.findAll('script')]

    # ' '.join(soup.findAll(text=True))
    # " ".join(soup.strings)
    # https://tuxcoder.wordpress.com/2010/09/14/how-to-get-the-webpage-content-how-to-extract-the-text-from-html-string-in-python/

    subscription_key_cognitive_service = ""  #
    assert subscription_key_cognitive_service
    text_analytics_base_url = "https://westcentralus.api.cognitive.microsoft.com/text/analytics/v2.0/"  # update it from panel settings

    # https://docs.microsoft.com/en-us/azure/cognitive-services/Text-Analytics/quickstarts/python
    # 1 Detect language
    language_api_url = text_analytics_base_url + "languages"
    print(language_api_url)
    documents = {'documents': [
        {'id': '1', 'text': 'This is a document written in English.'},
        {'id': '2', 'text': 'Este es un document escrito en Español.'},
        {'id': '3', 'text': '这是一个用中文写的文件'}
    ]}

    headers = {"Ocp-Apim-Subscription-Key": subscription_key_cognitive_service}
    response = requests.post(language_api_url, headers=headers, json=documents)
    languages = response.json()

    # 3 Key Phrase Extraction
    key_phrase_api_url = text_analytics_base_url + "keyPhrases"
    documents = {'documents': [
        {'id': '1', 'language': 'en',
         'text': 'I had a wonderful experience! The rooms were wonderful and the staff was helpful.'},
        {'id': '2', 'language': 'en',
         'text': 'I had a terrible time at the hotel. The staff was rude and the food was awful.'},
        {'id': '3', 'language': 'es',
         'text': 'Los caminos que llevan hasta Monte Rainier son espectaculares y hermosos.'},
        {'id': '4', 'language': 'es',
         'text': 'La carretera estaba atascada. Había mucho tráfico el día de ayer.'}
    ]}

    headers = {'Ocp-Apim-Subscription-Key': subscription_key_cognitive_service}
    response = requests.post(key_phrase_api_url, headers=headers, json=documents)
    key_phrases = response.json()
    # pprint(key_phrases)

    # Named Entity Recognition
    entity_linking_api_url = text_analytics_base_url + "entities"
    documents = {'documents': [
        {'id': '1', 'text': 'Jeff bought three dozen eggs because there was a 50% discount.'},
        {'id': '2', 'text': 'The Great Depression began in 1929. By 1933, the GDP in America fell by 25%.'}
    ]}
    headers = {"Ocp-Apim-Subscription-Key": subscription_key_cognitive_service}
    response = requests.post(entity_linking_api_url, headers=headers, json=documents)
    entities = response.json()
