import sys
import os

from whoosh.fields import Schema, TEXT, STORED
from whoosh.index import create_in, open_dir
from whoosh.query import *
from whoosh.qparser import QueryParser

#creating the schema
schema = Schema(tax_id=STORED,
                name=TEXT(stored=True))

#creating the index
if not os.path.exists("index"):
    os.mkdir("index")

ix = create_in("index",schema)
ix = open_dir("index")
writer = ix.writer()
writer.add_document(tax_id="17",name=u"Methyliphilus methylitrophus")
writer.add_document(tax_id="17",name=u"Methylophilus methylotrophus Jenkins et al. 1987")
writer.add_document(tax_id="45",name=u"Chondromyces lichenicolus") 
writer.commit()

myquery = And([Term("name",u"Chondromyces")])
with ix.searcher() as searcher:
    query = QueryParser("name", ix.schema).parse(u'Chondromyces')
    results = searcher.search(query)
    for result in results:
        print result