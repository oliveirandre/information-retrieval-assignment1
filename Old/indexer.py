# -*- coding: utf-8 -*-
import Stemmer
import os
import sys
import time
'''
Indexer work guide

Steps:
    1) Build a reader to read the Document Identifier (PMID) and Title (TI)
    2) Create a tokenizer with multiple aspects:
        2.1) Replaces non-alphabetic characters with a space, lowers cases everything and splits whitespaces.
        2.2) Ignore less than 3 characters long tokens.
        2.3) Pick what to do with special digits.
        2.4) Integrate the Porter stemmer (check online).
        2.5) Stopword Filter (list in the assignment).
    3) Create the indexer class to index corpus and save it to a file following the assignment formatting.
    4) Evaluate the following:
        4.1) Time it took to index (processing time) and the final file size (memory occupancy).
        4.2) Vocabulary size
        4.3) List the ten first terms (in alphabetic order) that appear in only one document (can have multiple term frequency in the same document)
        4.4) Ten terms with highest document frequency
    BONUS:
        1) Try to test multiple parameters so we improve the processing time and final file size.
        2) Store such values in order to achieve a comparison and explain it.

Parameters to implement:
    1) Verbose mode
    2) Stemmer cache size

    Data Structure:
            termo, documento1:frequência, documento2:frequência,...
Optional Extra:
    Threads to fulfill the computational power of reading/writting.

# TODO: Delete
    start_time = time.time()
    # your code
    elapsed_time = time.time() - start_time
'''
# Global Variables
current_line = 0
stemmer = Stemmer.Stemmer('english')
stemmer.maxCacheSize = 10000 # Default value, can be changed
stopwords = []
verbose = False
mean_time_aux = 0
mean_count_aux = 0

def main():
    global stopwords
    global verbose
    global mean_time_aux
    global mean_count_aux

    flag_reader(sys.argv)
    index = {}

    if verbose : print("[SYSTEM] Starting the indexer")
    stopwords = readStopwords()
    if verbose : print("[SYSTEM] Starting the indexing proccess")
    start_time = time.time()
    while True:
        # Reader
        t = reader()
        if not t: break
        # Tokenizer
        docs = {}
        docs["PMID"] = t[0]
        docs["TI"] = tokenizer(t[1])

        for value in docs["TI"]:
            index = indexer(docs["PMID"], value, index)

    elapsed_time = time.time() - start_time
    if verbose : print("[SYSTEM] Finished the indexing proccess, took: " + str(elapsed_time) + " s")
    if verbose : print("[SYSTEM] Starting the sorting proccess")
    start_time = time.time()
    sorter(index)
    elapsed_time = time.time() - start_time
    mean_read_time = mean_time_aux / mean_count_aux
    if verbose : print("[INFO] On average, reads took: " + str(mean_read_time) + " s")
    if verbose : print("[SYSTEM] Finished the sorting proccess, took: " + str(elapsed_time) + " s")

def flag_reader(args):
    global verbose
    if ('-help' in args):
        print("TODO")
        sys.exit(1)
    if ('-v' in args) or ('-V' in args):
        verbose = True


def reader():
    global current_line
    global mean_time_aux
    global mean_count_aux
    start_time = time.time()
    f = open("corpus182.txt", "r")
    pmid = ""
    ti = ""
    lines = f.readlines()
    for i in range(current_line, len(lines)):
        if "PMID" in lines[i]:
            pmid = lines[i].split("PMID- ")[1].split("\n")[0]
            current_line = i
            break
    for i in range(current_line, len(lines)):
        if "TI" in lines[i]:
            ti = lines[i]
            current_line = i
            break
    if pmid == "" or ti == "": return False
    elapsed_time = time.time() - start_time
    mean_time_aux += elapsed_time
    mean_count_aux += 1
    return (pmid, ti)

"""
DONE: 2.1) Replaces non-alphabetic characters with a space, lowers cases everything and splits whitespaces.
DONE: 2.2) Ignore less than 3 characters long tokens.
TODO: 2.3) Pick what to do with special digits.
DONE: 2.4) Integrate the Porter stemmer (check online).
DONE: 2.5) Stopword Filter (list in the assignment).
TODO: EXTRA) Count average time that took to proccess each line.
"""

def tokenizer(line):
    global stemmer
    global stopwords
    denied_chars = "!\"#$%&/()=?'|\\,;.:-_<>"
    for c in denied_chars:
        line = line.replace(c, " ")
    line = ' '.join(word for word in line.split() if len(word)>3)
    line = line.lower().split(" ")
    line = stemmer.stemWords(line)
    for w in line:
        if w in stopwords:
            line.remove(w)
    return line

def readStopwords():
    f = open("snowball_stopwords_EN.txt", "r")
    s = f.read()
    return s.split("\n")

def postingById(index, term):
    if term in index:
        return index[term]
    else:
        return ()

def postingByDoc(postings, docid):
    for post in postings:
        if post == docid:
            return True
    return False

def indexer(docid, term, index):
    frequency = 1

    if term not in index:
        index[term] = [(docid, frequency)]

    else:
        postings = postingById(index, term)
        post = postingByDoc(postings, docid)

        if not post:
            index[term].append(tuple((docid, frequency)))

        else:
            #print("FOUND ONE")
            for p in index[term]:
                if p[0] == docid:
                    lst = list(p)
                    index[term].remove(p)
                    lst[1] = lst[1] + 1
                    t = tuple(lst)
                    index[term].append(tuple(t))

    return index

def sorter(index):
    f = open("results.txt", "w")
    for key in sorted(index.keys()):
        f.write(str([key, index[key]]))
        f.write("\n")
    f.close()


if __name__ == "__main__":
    main()
