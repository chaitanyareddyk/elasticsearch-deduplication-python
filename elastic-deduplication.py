from elasticsearch import Elasticsearch
try:
    es = Elasticsearch(
        ['http://localhost'],
        http_auth=('user_name', 'password'),
        port=9200,
    )
    print("Connected", es.info())
except Exception as ex:
    print("Error:", ex)

duplicatesDict = {}


def addToDuplicateDict(hits):
    for item in hits:
        if 'YOUR_KEY' in item['_source']:
            key = str(item['_source']['YOUR_KEY'])
            _id = item["_id"]
            duplicatesDict.setdefault(key, []).append(_id)
        else:
            continue


def fetchAllDocs():
    data = es.search(index="your_index", scroll='1m',
                     body={"query": {"match_all": {}}})
    sid = data['_scroll_id']
    scrollSize = len(data['hits']['hits'])
    addToDuplicateDict(data['hits']['hits'])
    while scrollSize > 0:
        data = es.scroll(scroll_id=sid, scroll='2m')
        addToDuplicateDict(data['hits']['hits'])
        sid = data['_scroll_id']
        scrollSize = len(data['hits']['hits'])


def removeDuplicates():
    duplicates_count = 0
    for key, array_of_ids in duplicatesDict.items():
        if len(array_of_ids) > 1:
            duplicates_count += 1
            print(
                "----------------------- Deleting duplicate documents with key= %s -----------------------" % key)
            matching_docs = es.mget(index="your_index", doc_type="doc", body={
                                    "ids": array_of_ids})
            for doc in matching_docs['docs'][1:]:
                print("Deleting doc with id: ", doc['_id'])
                es.delete(index="your_index", id=doc['_id'])
    print("Removed %d duplicates" % duplicates_count)


def main():
    fetchAllDocs()
    removeDuplicates()


main()
