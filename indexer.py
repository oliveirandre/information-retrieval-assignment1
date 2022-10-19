# coding: utf-8

"""
Authors:
- André Oliveira, nº79969
- Dinis Canastro, nº80299
"""

from tokenizer import Tokenizer
import operator

class Indexer:
    def __init__(self):
        self.index = {}

    # Deprecated
    def addToIndex(self,docid, term):
        frequency = 1

        if term not in self.index:
            self.index[term] = [(docid, frequency)]
        else:
            postings = self.postingById(self.index, term)
            post = self.postingByDoc(postings, docid)
            if not post:
                self.index[term].append(tuple((docid, frequency)))
            else:
                for p in self.index[term]:
                    if p[0] == docid:
                        lst = list(p)
                        self.index[term].remove(p)
                        lst[1] = lst[1] + 1
                        t = tuple(lst)
                        self.index[term].append(tuple(t))
        return

    # Deprecated
    def postingById(self, index, term):
        if term in self.index:
            return self.index[term]
        else:
            return ()

    # Deprecated
    def postingByDoc(self, postings, docid):
        for post in postings:
            if post[0] == docid:
                return True
        return False

    def sortIndex(self):
        pass

    # Deprecated
    def addToIndex2(self,docid, term):
        count = 0
        if term in self.index:
            for docs in self.index[term]:
                if docid == docs[0]:
                    self.index[term][count][1] += 1
                    return
                count += 1
            self.index[term].append([docid,1])
        else:
            self.index[term] = [[docid,1]]
        return

'''
    Optimized indexer:
        Since we use a pair PMID and TI we can consider there will be no further processing on the same document (same PMID).
        As such we skip the verifying if each term has already an entry with such PMID.
        This allowed us to improve a lot on the index creation.
'''
    def addToIndex3(self,docid, values):
        #Criar dicionário temporário desta entrada
        temp_index = {}
        count = 0
        for v in values:
            if v in temp_index:
                temp_index[v][1] += 1
            else:
                temp_index[v] = [docid,1]
        #Dar append/criar ao index geral
        for i in temp_index:
            if i in self.index:
                self.index[i].append(temp_index[i])
            else:
                self.index[i] = [temp_index[i]]
        return

    # Sending the reference to the instance so it doesn't require context changes every single pair.
    def addAll(self, all, t):
        count = 0
        size = len(all)
        while all:
            count += 1
            if (count % 25000 == 0): print("%.2f" % (count / size * 100))
            tmp = all.pop()
            self.addToIndex3(tmp[0], t.tokenize(tmp[1], 3))

'''
    Method that takes a file_name as input, where the inverse index is written to.
'''
    def writeToFile(self, file_name):
        f = open(file_name, "w")
        print(len(self.index.keys()))
        # Writes in file the sorted index (i.e. the inverse index)
        for key in sorted(self.index.keys()):
            f.write(str(key) + ":")
            for value in self.index[key]:
                f.write(value[0] + "," + str(value[1]) + ";")
            f.write("\n")
        f.close()

'''
    Methods that answer the questions asked on the assignment.
'''
    # Adds to a list the first ten tokens (alphabeticly) that have document frequency equal to one.
    def questionThree(self):
        l = []
        i = 0
        for key in sorted(self.index.keys()):
            if i > 9:
                break
            flag = True
            if len(self.index[key]) > 1:
                flag = False
            if flag:
                l.append(key)
                i += 1
        return str(l)

    # Dictionary that contains the ten terms with the highest document frequency.
    # The dictionary is sorted from lower to higher frequencies.
    def questionFour(self):
        l = []
        i = 0
        for key in sorted(self.index.keys()):
            if i < 10:
                l.append(key)
                i += 1
                if i == 10:
                    for j in range(0, len(l)-1):
                        if len(self.index[l[j]]) > len(self.index[l[j+1]]):
                            l[j], l[j+1] = l[j+1], l[j]
            else:
                if len(self.index[key]) > len(self.index[l[len(l)-1]]):
                    l[0:len(l)-1] = l[1:len(l)]
                    l[len(l)-1] = key

                else:
                    for k in range(0, len(l)):
                        if len(self.index[key]) <= len(self.index[l[k]]):
                            if len(self.index[key]) > len(self.index[l[k-1]]):
                                l[0:k-1] = l[1:k]
                                l[k-1] = key
        d = {}
        for m in range(0, len(l)):
            d[l[m]] = len(self.index[l[m]])
        return str(d)

    def printIndex(self):
        for i in sorted(self.index.keys()):
            print(i + ":" + str(self.index[i]))

'''
    Compression: methods take file_name as input, on which the compressed index is written to.
    Two modes: one with and one without blocking.
'''
    def dictionaryAsString(self, file_name):
        f = open(file_name, "w")
        s = ""
        d = {}
        i = 0
        for key in sorted(self.index.keys()):
            s += key
        f.write(s)
        f.write("\n")
        for key in sorted(self.index.keys()):
            d[i] = self.index[key]
            f.write(str(i) + ":")
            for value in d[i]:
                f.write(value[0] + "," + str(value[1]) + ";")
            f.write("\n")
            i = i + len(key)

    def blocking(self, file_name):
        f = open(file_name, "w")
        s = ""
        d = {}
        for key in sorted(self.index.keys()):
            s += str(len(key)) + key
        f.write(s)
        f.write("\n")
        for key in sorted(self.index.keys()):
            i = len(key)
            d[i] = self.index[key]
            f.write(str(i) + ":")
            for value in d[i]:
                f.write(value[0] + "," + str(value[1]) + ";")
            f.write("\n")
