import os

DEBUG = 0

class solution():
    def __init__(self):
        if DEBUG == 1:
            self.fileName =  'test.txt'
        else:
            self.fileName =  'content.txt'
        self.key = {'asm', 'do', 'if', 'return',
                    'typedef', 'auto', 'double',
                    'inline', 'short', 'typeid',
                    'bool', 'dynamic_cast', 'int',
                    'signed', 'typename', 'break',
                    'else', 'long', 'sizeof', 'union',
                    'case', 'enum', 'mutable', 'static',
                    'unsigned', 'catch', 'explicit',
                    'namespace', 'static_cast', 'using',
                    'char', 'export', 'new', 'struct',
                    'virtual', 'class', 'extern',
                    'operator', 'switch', 'void',
                    'const', 'false', 'private',
                    'template', 'volatile', 'const_cast',
                    'float', 'protected', 'this',
                    'wchar_t', 'continue', 'for',
                    'public', 'throw', 'while',
                    'default', 'friend', 'register',
                    'true', 'delete', 'goto',
                    'reinterpret_cast', 'try'}
    
    def showStack(self, stack):
        for i in xrange(len(stack)):
            print stack[i],
            print ' ',
        if len(stack) > 0:
            print
    
    def createDir(self, pathDir):
        getNew = 'new'+pathDir
        if not os.path.exists(getNew):
            os.mkdir(getNew)
            
    def handleH(self, oneFile, newFile):
        one = open(oneFile, 'r')
        new = open(newFile, 'w')
        for line in one.readlines():
            print >> new, line.strip()
        one.close()
        new.close()
        
    def handleHpp(self, oneFile, newFile):
        one = open(oneFile, 'r')
        new = open(newFile, 'w')
#        test = open("result.txt", "w")
        edge = 1
        pre = 0
        cur = 0
        tmp = 0
        stack = list()
        for line in one.readlines():
            data = line.strip()
#            print >>  test, data, "edge =", edge, "pre =", pre, "cur =", cur, "len =", len(stack), "tmp =", tmp
            if data[:9] == 'namespace' and cur == 0:
                edge = 3
            elif data[:5] == 'class' and cur == 0:
                edge = 2
            elif data[:6] == 'struct':
#                print data
                stack.append(cur)
                tmp = cur
            if len(stack) > 0:    #for the struct
                for i in xrange(len(data)):
                    if data[i] == '{':
                        tmp += 1
                    elif data[i] == '}':
                        tmp -= 1
                if tmp != cur:
                    print >> new, data
                    continue
                cur = stack.pop()
                tmp = cur
                if len(stack) > 0:
                    for i in xrange(len(data)):
                        if data[i] == '{':
                            tmp += 1
                        elif data[i] == '}':
                            tmp -= 1
                    print >> new, data
                    continue
            for i in xrange(len(data)):
                if data[i] == '{':
                    cur += 1
                elif data[i] == '}':
                    cur -= 1
            if cur <= (edge-1) or (pre == (edge-1) and cur >= edge):
                print >> new, data
            pre = cur
            if cur == 0:
                edge = 1
        one.close()
        new.close()
#        test.close()
        
    def clean(self):
        f = open(self.fileName, 'r')
        for i in f.readlines():
            tmp = i.strip()
            tag = 0
            for j in xrange(len(tmp)):
                if tmp[j] == '.':
                    tag = 1
            if tag == 0:
                self.createDir(tmp)
            oneFile = 'all'
            newFile = 'new'
            if tmp[-3:] == 'hpp' or tmp[-3:] == 'cpp' or tmp[-2:] == '.c':
                oneFile += tmp
                newFile += tmp
                self.handleHpp(oneFile, newFile)
            elif tmp[-2:] == '.h':
                oneFile += tmp
                newFile += tmp
                self.handleHpp(oneFile, newFile)
        f.close()
a = solution()
a.clean()