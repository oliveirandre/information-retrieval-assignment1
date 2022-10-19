import sys
import os

"""
Authors:
- André Oliveira, nº79969
- Dinis Canastro, nº80299
"""

"""
    Implementation of a reader that buffers the whole file to the memory (ideal situation considering the host system has lots of memory).
"""
class Reader:
    def __init__(self, file_name):
        self.f = open(file_name, "r", encoding="utf8", errors='ignore')
        self.lines = self.f.readlines()
        self.current_line = 0

    def returnPair(self):
        pmid = ""
        ti = ""
        for i in range(self.current_line, len(self.lines)):
            if (i % 1000000 == 0) : print("%.2f" % (i / len(self.lines) * 100))
            if "PMID" == self.lines[i][0:4]:
                try:
                    pmid = self.lines[i].split("PMID- ")[1].split("\n")[0]
                    self.current_line = i
                    break
                except:
                    print(self.lines[i])
                    
        for i in range(self.current_line, len(self.lines)):
            if "TI" == self.lines[i][0:2]:
                ti = self.lines[i]
                self.current_line = i
                break
        if pmid == "" or ti == "": return False
        return (pmid, ti)

    def returnAll(self):
        all = []
        temp = ""
        count = 0
        for i in self.lines:
            count += 1
            if (count % 1000000 == 0): print("%.2f" % (count / len(self.lines) * 100))
            if "PMID" == i[0:4]:
                temp = i.split("PMID- ")[1].split("\n")[0]
            if "TI" == i[0:2]:
                all.append([temp, i])
        return all

    def __del__(self):
        self.f.close()
        del self.lines

"""
    Implementation of a reader that buffers line by line (ideal for low memory environments).
"""
class Reader2:
    def __init__(self, file_name):
        self.f = open(file_name, "r", encoding="utf8", errors='ignore')
        self.count = 0
        # self.lines = self.f.readlines()
        # self.current_line = 0

    def returnPair(self):
        pmid = ""
        ti = ""
        line = "Starting"
        while line != "":
            line = self.f.readline()
            self.count += 1
            # if (self.count % 10000000 == 0) : print("%.2f" % (self.count / self.size * 100))
            if "PMID" == line[0:4]:
                pmid = line.split("PMID- ")[1].split("\n")[0]
                break
        while line != "":
            line = self.f.readline()
            self.count += 1
            if "TI" == line[0:2]:
                ti = line
                break
        if pmid == "" or ti == "": return False
        #print(pmid + "- " + ti)
        return (pmid, ti)

    def __del__(self):
        self.f.close()

