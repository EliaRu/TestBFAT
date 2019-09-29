import subprocess as subp

l = 1
r = -60

testProgram = "test_inc"
for w in [1000, 10000, 100000]:
    graphFilename = "./graphs/{}_{}.dat".format( testProgram, w )
    print( graphFilename )
    graphFile = open( graphFilename, "w" )
    for s in range( 1, 11 ):
        command = "./{} -l {} -r {} -w {} -s {} --use-ff".format( testProgram, l, r, w, s )
        print( command )
        args = command.split( )
        result = subp.run( args, stdout=subp.PIPE, universal_newlines=True)
        output = result.stdout
        lines = output.splitlines( )
        graphFile.write( str( s ) )
        graphFile.write( "\t" )
        graphFile.write( lines[1].split( )[-1] )
        graphFile.write( "\t" )
        graphFile.write( lines[-1].split( )[-1] )
        graphFile.write( "\n" )


"""
for w in [1000, 10000, 100000]:
    graphFilename = "./graphs/{}_{}_stddev.dat".format( testProgram, w )
    print( graphFilename )
    graphFile = open( graphFilename, "w" )
    for s in range( 1, 11 ):
        command = "./{} -l {} -r {} -w {} -s {} --operator sample-stddev --use-ff".format( testProgram, l, r, w, s )
        print( command )
        args = command.split( )
        result = subp.run( args, stdout=subp.PIPE, universal_newlines=True)
        output = result.stdout
        lines = output.splitlines( )
        graphFile.write( str( s ) )
        graphFile.write( "\t" )
        graphFile.write( lines[1].split( )[-1] )
        graphFile.write( "\t" )
        graphFile.write( lines[-1].split( )[-1] )
        graphFile.write( "\n" )
"""

testProgram = "test_fat"
for w in [1000, 10000, 100000]:
    graphFilename = "./graphs/{}_{}.dat".format( testProgram, w )
    print( graphFilename )
    graphFile = open( graphFilename, "w" )
    for s in range( 1, 11 ):
        command = "./{} -l {} -r {} -w {} -s {} --use-ff".format( testProgram, l, r, w, s )
        print( command )
        args = command.split( )
        result = subp.run( args, stdout=subp.PIPE, universal_newlines=True)
        output = result.stdout
        lines = output.splitlines( )
        graphFile.write( str( s ) )
        graphFile.write( "\t" )
        graphFile.write( lines[1].split( )[-1] )
        graphFile.write( "\t" )
        graphFile.write( lines[-1].split( )[-1] )
        graphFile.write( "\n" )

testProgram = "test_bfat"
for w in [1000, 10000, 100000]:
    for b in [500, 1000, 5000, 10000 ]:
        graphFilename = "./graphs/{}_{}_b{}.dat".format( testProgram, w, b )
        print( graphFilename )
        graphFile = open( graphFilename, "w" )
        for s in range( 1, 11 ):
            command = "./{} -l {} -r {} -w {} -s {} -b {} --use-ff".format( testProgram, l, r, w, s, b)
            print( command )
            args = command.split( )
            result = subp.run( args, stdout=subp.PIPE, universal_newlines=True)
            output = result.stdout
            lines = output.splitlines( )
            graphFile.write( str( s ) )
            graphFile.write( "\t" )
            graphFile.write( lines[1].split( )[-1] )
            graphFile.write( "\t" )
            graphFile.write( lines[-1].split( )[-1] )
            graphFile.write( "\n" )

for w in [1000, 10000, 100000]:
    for b in [ 500, 1000, 5000, 10000 ]:
        graphFilename = "./graphs/{}_{}_b{}_rebuilt.dat".format( testProgram, w, b )
        print( graphFilename )
        graphFile = open( graphFilename, "w" )
        for s in range( 1, 11 ):
            command = "./{} -l {} -r {} -w {} -s {} -b {} --rebuild --use-ff".format( testProgram, l, r, w, s, b)
            print( command )
            args = command.split( )
            result = subp.run( args, stdout=subp.PIPE, universal_newlines=True)
            output = result.stdout
            lines = output.splitlines( )
            graphFile.write( str( s ) )
            graphFile.write( "\t" )
            graphFile.write( lines[1].split( )[-1] )
            graphFile.write( "\t" )
            graphFile.write( lines[-1].split( )[-1] )
            graphFile.write( "\n" )
