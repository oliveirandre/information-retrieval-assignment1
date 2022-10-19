# coding: utf-8

"""
Authors:
- André Oliveira, nº79969
- Dinis Canastro, nº80299
"""


import Stemmer
import os
import sys
import time
from indexer import Indexer
from reader import Reader
from reader import Reader2
from tokenizer import Tokenizer
import threading
import gc

verbose = False
threads = False
low_memory = True
compression1 = False
compression2 = False
min_token_size = 3
stemmer_cache = 10000

"""
Statistics:
    nº de PMIDs: 4,995,122
    nº de TI's no Doc 1: 2295504
    nº de TI's no Doc 2: 2295504

Notes:
    Tempo não melhora usando High Memory + Threading mode portanto essa opção foi cortada (requeria neste caso 24Gb de RAM)
"""

def main():
    # Global flags used for the various operation modes
    global verbose
    global threads
    global low_memory
    global compression1
    global compression2
    global min_token_size
    global stemmer_cache

    gc.disable() # We require manual garbage collection during runtime for improved performance.

    # Method to read the arguments
    readCLArguments(sys.argv)

    if verbose : print("[SYSTEM] Starting the program")
    # Creating the required objects
    i = Indexer()
    t = Tokenizer(stemmer_cache,min_token_size)
    times = [] # Will be used to store the times of all the phases for the total value at the end.

    # This section will run if we don't use the threaded option
    if not threads:
        # This section will run if we use the high memory option (will take abour 12Gb of RAM from our tests)
        if low_memory == False:
            if verbose: print("[SYSTEM] Running in high memory profile (recomended 16Gb of RAM)")
            if verbose: print("[SYSTEM] Starting the indexing proccess of part 1")
            start_time = time.time()

            # Starting the reading process
            r1 = Reader("2004_TREC_ASCII_MEDLINE_1")
            all = r1.returnAll()
            del r1 # We don't require the reader no more so temporarily we will delete him so we leave more memory available

            elapsed_time = time.time() - start_time
            times.append(elapsed_time)
            if verbose: print("[SYSTEM] Finished the reading proccess, took: " + str(elapsed_time) + " s")
            if verbose: print("[SYSTEM] Starting the indexing and tokenizing process")
            # Starting the indexing and tokenizing process
            print("Number of documents found: " + str(len(all)))
            start_time = time.time()

            i.addAll(all, t) # The tokenizer reference is sent to the class so we loose less time in context changing (from our tests we saw improvement)
            elapsed_time = time.time() - start_time
            times.append(elapsed_time)
            if verbose: print("[SYSTEM] Finished the indexing proccess, took: " + str(elapsed_time) + " s")
            if verbose: print("[SYSTEM] Finished everything, took: " + str(sum(times)) + " s")

            # ------------------ Part 2 (is the same) ----------------
            if verbose: print("[SYSTEM] Starting the indexing proccess of part 2")
            start_time = time.time()

            # Starting the reading process
            r2 = Reader("2004_TREC_ASCII_MEDLINE_2")
            all = r2.returnAll()
            del r2

            elapsed_time = time.time() - start_time
            times.append(elapsed_time)
            if verbose: print("[SYSTEM] Finished the reading proccess, took: " + str(elapsed_time) + " s")
            if verbose: print("[SYSTEM] Starting the indexing and tokenizing process")
            # Starting the indexing and tokenizing process
            if verbose: print("[INFO] Number of documents found: " + str(len(all)))
            start_time = time.time()

            i.addAll(all, t)
            elapsed_time = time.time() - start_time
            times.append(elapsed_time)
            if verbose: print("[SYSTEM] Finished the indexing proccess, took: " + str(elapsed_time) + " s")
            if verbose: print("[SYSTEM] Finished everything, took: " + str(sum(times)) + " s")
        else:
            # This section is the low memory option (will require less than 2Gb of ram from our tests)
            if verbose: print("[SYSTEM] Running in low memory profile (for systems below 16Gb of RAM)")
            if verbose: print("[SYSTEM] Starting the indexing proccess of part 1")
            start_time = time.time()
            count = 0
            # Starting the reading section
            r1 = Reader2("2004_TREC_ASCII_MEDLINE_1")
            pair = "First"
            while True:
                # Will read a pair at a time and introduce him in the index
                count += 1
                pair = r1.returnPair()
                if not pair: break
                i.addToIndex3(pair[0], t.tokenize(pair[1]))
            elapsed_time = time.time() - start_time
            times.append(elapsed_time)
            if verbose: print("[SYSTEM] Finished the reading/indexing proccess, took: " + str(elapsed_time) + " s")
            if verbose: print("[INFO] Number of documents found: " + str(count))
            # ------------------ Part 2 (is the same) ----------------
            if verbose: print("[SYSTEM] Starting the indexing proccess of part 2")
            start_time = time.time()
            count = 0
            r1 = Reader2("2004_TREC_ASCII_MEDLINE_2")
            pair = "First"
            while True:
                # Reader
                count += 1
                pair = r1.returnPair()
                if not pair: break
                i.addToIndex3(pair[0], t.tokenize(pair[1]))
            elapsed_time = time.time() - start_time
            times.append(elapsed_time)
            if verbose: print("[SYSTEM] Finished the reading/indexing proccess, took: " + str(elapsed_time) + " s")
            if verbose: print("[INFO] Number of documents found: " + str(count))

    else:
        # This section is used in the threaded option (we saw no improvement over the regular high memory one on our tests)
        if verbose: print("[SYSTEM] Starting the indexing proccess through threads")
        start_time = time.time()
        thread_lst = list()
        # Launching threads with ID 1 and 2
        for index in range(1,3):
            tmp = threading.Thread(target=asyncReader, args=(index,t,i,))
            thread_lst.append(tmp)
            tmp.start()
        # Waiting for threads to end so we can write to file
        for index, thread in enumerate(thread_lst):
            thread.join()
        elapsed_time = time.time() - start_time
        times.append(elapsed_time)
        if verbose: print("[SYSTEM] Finished indexing, took: " + str(elapsed_time) + " s")

    if verbose:
        print("List the ten first terms (in alphabetic order) that appear in only one document (document frequency = 1).")
        print(i.questionThree())

        print("\nList the ten terms with highest document frequency.")
        print(i.questionFour())

    #i.printIndex()
    if verbose: print("[SYSTEM] Starting to write index on file.")
    start_time = time.time()

    if compression1:
        i.dictionaryAsString("dictionaryAsString.txt")
    if compression2:
        i.blocking("blocking.txt")
    if not compression1 and not compression2:
        i.writeToFile("index.txt")

    elapsed_time = time.time() - start_time
    times.append(elapsed_time)
    if verbose: print("[SYSTEM] Finished the writing proccess, took: " + str(elapsed_time) + " s")
    if verbose: print("[SYSTEM] Finished everything, took: " + str(sum(times)) + " s")
    gc.enable()

def asyncReader(index,t,i):
    # Exactly the same has the low memory mode otherwise we would be spending 24Gb of RAM without any noticeable improvement.
    global verbose
    print("[SYSTEM] Starting thread: " + str(index))
    pair = "First"
    count = 0
    r1 = Reader2("2004_TREC_ASCII_MEDLINE_" + str(index))
    pair = "First"
    while True:
        # Reader
        count += 1
        pair = r1.returnPair()
        if not pair: break
        i.addToIndex3(pair[0], t.tokenize(pair[1]))
    ("[INFO] Thread " + str(index) + " found " + str(count) + " documents.")
    print("[SYSTEM] Finished thread: " + str(index))


'''
    Functions to process command line arguments
'''
def readCLArguments(args):
    global verbose
    global threads
    global low_memory
    global compression1
    global compression2
    global min_token_size
    global stemmer_cache

    if ('--help' in args):
        printHelp()
    if ('-v' in args) or ('-V' in args):
        verbose = True
    if ('-t' in args) or ('-T' in args):
        threads = True
    if ('-h' in args) or ('-H' in args):
        low_memory = False
    if ('-c1' in args) or ('-C1' in args):
        compression1 = True
    if ('-c2' in args) or ('-C2' in args):
        compression2 = True
    if ('-s' in args):
        if args[args.index('-b') + 1].isdigit():
            stemmer_cache = int(args[args.index('-b') + 1])
        else:
            printHelp()
    if ('-m' in args):
        if args[args.index('-m') + 1].isdigit():
            min_token_size = int(args[args.index('-m') + 1])
        else:
            printHelp()



def printHelp():
    print("usage: python3 main.py [option]")
    print("Options:")
    print("-v           : verbose mode")
    print("-t           : use threads (can't be used with high memory mode)")
    print("-h           : use high memory mode (low-memory is default)")
    print("-c1          : use compression mode 1")
    print("-c2          : use compression mode 2")
    print("-s [number]  : change Stemmer Cache size (default: 10000)")
    print("-m [number]  : change tokens minimum accepted size (default: 3)")
    sys.exit(1)

if __name__ == "__main__":
    main()
