from firebase_admin import firestore
import json
import mwe_extractor
from datetime import datetime, timezone
from google.cloud import storage

fire_db = firestore.Client()


# loads all articles for a given collection for above defined time period
def get_articles(col, fr, to):
    art_stream = fire_db.collection('news').document('articles').collection(col).where("time_stamp", ">=", fr).where(
        "time_stamp", "<=", to).stream()
    arts = []
    for art_ref in art_stream:
        art = art_ref.to_dict()
        arts.append(art)
    return arts


# iterates over the collections to load the articles
def iterate_collections(fr, to):
    colls = fire_db.collection('news').document('articles').collections()
    ll = list(colls)
    all_articles = []
    for col in ll:
        my_articles = get_articles(col.id, fr, to)
        all_articles.extend(my_articles)
    return all_articles


def extract(fr, to):
    all_articles = iterate_collections(fr, to)
    mwes = mwe_extractor.extract_mwes(all_articles)
    return mwes


def store_in_bucket(mwe, to, fr, kind):
    date_str = str(datetime.fromtimestamp(to)).replace(" ", "_")
    file_name = kind + '-' + date_str + '.json'
    my_dict = {'from_date': fr, 'to_date': to, 'mwes': list(mwe)}
    with open("/tmp/tmp.json", "w") as tmp:
        json.dump(my_dict, tmp)
    bucket_name = "semantic_features"
    source_file_name = "/tmp/tmp.json"
    destination_blob_name = "mwes/" + file_name
    current_blob_name = "mwes/" + kind + "_current.json"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    blob = bucket.blob(current_blob_name)
    blob.upload_from_filename(source_file_name)


def load_global():
    file_name = 'cum_current.json'
    storage_client = storage.Client()
    destination_file_name = "/tmp/tmp.json"
    bucket_name = "semantic_features"
    bucket = storage_client.bucket(bucket_name)
    blob_name = "mwes/" + file_name
    blob = bucket.blob(blob_name)
    blob.download_to_filename(destination_file_name)

    with open(destination_file_name, encoding='utf8') as json_file:
        mwes = json.load(json_file)
    return mwes


def run():
    global_mwe_obj = load_global()
    fr = global_mwe_obj['to_date']
    global_fr = global_mwe_obj['from_date']
    global_mwe = set(global_mwe_obj['mwes'])
    to = datetime.now(tz=None).replace(tzinfo=timezone.utc).timestamp()
    my_mwe = extract(fr, to)
    if len(my_mwe) == 0:
        print("no new mwes found. try later.")
        return
    my_new_mwe = my_mwe - global_mwe
    store_in_bucket(my_new_mwe, to, fr, 'incr')
    global_mwe.update(my_new_mwe)
    store_in_bucket(global_mwe, to, global_fr, 'cum')

