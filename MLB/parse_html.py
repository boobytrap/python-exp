import urllib.request
import requests
from lxml import html
from lxml.html import parse
from lxml import etree


ar = ['https://www.mlb.com/standings/']
dict = {}

for url in ar:
        try:
                #req = urllib.request.Request(url,headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'})
                req = requests.get(url)
                #f = urllib.request.urlopen(req)
        except urllib.error.URLError:
                    pass
        else:
                tree = html.fromstring(req.content)
                header = tree.find('.//table').text
                print(header)
                print(tree[0].text)
                #p = parse(f)
                #dict[url] = {"title": p.find(".//title").text}
                #dict[url] = {"head": p.find(".//head").text}

#print(dict)
#print(html.tostring(tree, pretty_print=True))
