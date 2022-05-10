from functools import wraps
import os


def atomic_writing_file(inputfile,outputfile):
    def atomic_writing_decorator(func):
            @wraps(func)
            def atomicw(*args, **kwargs):
                namelen = len(outputfile)-1
                tmpfile = outputfile+"tmp"
                while namelen >= 0:
                    if outputfile[namelen] == ".":
                        tmpfile = outputfile[:namelen]+"tmp"+outputfile[namelen:]
                        break
                    else:
                        namelen -=1
                try:
                    fw = open(tmpfile,'x')
                    with open(inputfile) as fo:
                        while True:
                            line = fo.readline()
                            if not line: break
                            fw.write(line)
                    
                    fw.flush()
                    os.fsync(fw.fileno())
                    fo.close()
                    fw.close()
                    os.rename(tmpfile, outputfile)
                except:
                    print("Oops, an error was occured...")
                    os.remove(tmpfile)

                return func(*args, **kwargs)
            return atomicw
    return atomic_writing_decorator

@atomic_writing_file(inputfile="log.txt",outputfile="test.txt")
def atomic_writing():
    print("Your work is done!")
    return True


"""
version1.0, decorator with no parameter.

def atomic_writing_decorator(func):
        @wraps(func)
        def atomicw(*args, **kwargs):
            inputfile = str(input("input your parquet file path and name: "))
            outputfile = str(input("input your outputfile path and name: "))
            namelen = len(outputfile)-1
            tmpfile = outputfile+"tmp"
            while namelen >= 0:
                if outputfile[namelen] == ".":
                    tmpfile = outputfile[:namelen]+"tmp"+outputfile[namelen:]
                    break
                else:
                    namelen -=1
            
            fw = open(tmpfile,'x')
            with open(inputfile) as fo:
                while True:
                    line = fo.readline()
                    if not line: break
                    fw.write(line)
            
            fw.flush()
            os.fsync(fw.fileno())
            fo.close()
            fw.close()
            os.rename(tmpfile, outputfile)

            #print("inputfilename: ",inputfile)
            #print("outputfilename: ", tmpfile)
            return func(*args, **kwargs)
        return atomicw

@atomic_writing_decorator
def atomic_writing():
    print("done")

"""
    
#if __name__ == "__main__":
    #inputfile = input("input your parquet file path and name: ")
    #outputfile = input("input your outputfile path and name: ")
#    atomic_writing()
