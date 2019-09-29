#include <getopt.h>
#include <iostream>
#include <string>

#include <windflow.hpp>

#include "common.hpp"

using namespace std;

int main( int argc, char *argv[] )
{
    size_t stream_len;
    int64_t rate;
    size_t win_len;
    size_t slide;

    size_t num_keys = 1;
    size_t num_workers = 1;
    size_t num_sources = 1;
    size_t extra = 0;
    bool rebuildFAT = false;
    uint64_t quantum = 0;
    bool fakeTB = false;
    bool useFF = false;
    operator_t<tuple_t> functions = {
        SumLift, SumCombine, SumLower, true
    };

    int option = 0;
    int option_index = 0;
    int num_mandatory_parameters = 0;
    string cmd_message = argv[0];
    cmd_message += " -l <stream length> -r <rate> -w <win length> -s <win slide> [-k <keys number>] [-p <workers number>] [--num-sources <sources number>] [--extra-work <time>] [--quantum <time>] [--operator <operator>] [--use-ff]\n";
    while( 1 ) {
        static struct option long_options[] = {
            { "num-sources",    required_argument,  0, 0 },
            { "extra-work",     required_argument,  0, 1 },
            { "quantum",        required_argument,  0, 2 },
            { "operator",       required_argument,  0, 3 },
            { "use-ff",         no_argument,        0, 4 },
            { 0,                0,                  0, 0 }
        };

        option = getopt_long( argc, argv, "hl:r:w:s:k:p:", 
                    long_options, &option_index );
        if( option == -1 ) {
            break;
        }
        switch( option ) {
            case 0:
                num_sources = atoi( optarg ); 
                break;
            case 1:
                extra = atoi( optarg );
                break;
            case 2:
                quantum = strtoull( optarg, NULL, 10 );
                fakeTB = true;
                break;
            case 3:
                if( GetOperator( optarg, functions ) == -1 ) {
                    return 0;
                }
                break;
            case 4:
                useFF = true;
                break;
            case 'h':
                cout << cmd_message;
                cout << "Supported operations:" << endl;
                cout << "sum, count, max, arithmetic-mean, geometric-mean," 
                << " max-count, sample-stddev, arg-max" << endl;
                return 0;
            case 'l':
                stream_len = atoi( optarg );
                num_mandatory_parameters++;
                break;
            case 'r':
                rate = atoi( optarg );
                num_mandatory_parameters++;
                break;
            case 'w':
                win_len = atoi( optarg );
                num_mandatory_parameters++;
                break;
            case 's':
                slide = atoi( optarg );
                num_mandatory_parameters++;
                break;
            case 'k':
                num_keys = atoi( optarg );
                break;
            case 'p':
                num_workers = atoi( optarg );
                break;
            default:
                cout << cmd_message;
                return 0;
        }
    }
    if( num_mandatory_parameters != 4 ) {
        cout << cmd_message;
        return 0;
    }

    if( useFF ) {
        Generator gen( stream_len, rate, num_keys );
        Source source = Source_Builder( gen )
                        .withName( "generator" )
                        .build( );

        Consumer consumer( num_keys, functions );
        Sink sink = Sink_Builder( consumer ).withName( "sink" ).build( );

        ff_pipeline pipe;
        pipe.add_stage( &source );
        pipe.add_stage( &sink );

        cout << "Eseguito pipeline con " << pipe.cardinality() << " threads" << endl;
        unsigned long t0 = current_time_nsecs( );
        if (pipe.run_and_wait_end() < 0) {
                cerr << "Error execution of ff_pipe" << endl;
                return -1;
        }
        else {
                cout << "...end ff_pipe" << endl;
        }
        unsigned long t1 = current_time_nsecs( );
        double dt = ( t1 - t0 ) / 1000000.0;
        cout << "Tempo di completamento: " << dt << " ms." << endl;

        double tuples_per_sec = atomic_load( &num_tuples_generated ) / ( dt / 1000.0 );
        cout << "Media elementi processati al secondo: " << tuples_per_sec 
        <<  endl;

    } else {
        Generator gen( stream_len, rate, num_keys );
        Source source = Source_Builder( gen )
                        .withParallelism( num_sources )
                        .withName( "generator" )
                        .build( );


        Consumer consumer( num_keys, functions );
        Sink sink = Sink_Builder( consumer ).withName( "sink" ).build( );

        MultiPipe pipe( "Test con Key Farm e FAT" );
        pipe.add_source( source );
        pipe.add_sink( sink );

        unsigned long t0 = current_time_nsecs( );
        if (pipe.run_and_wait_end() < 0) {
                cerr << "Error execution of ff_pipe" << endl;
                return -1;
        }
        else {
                cout << "...end ff_pipe" << endl;
        }
        unsigned long t1 = current_time_nsecs( );
        double dt = ( t1 - t0 ) / 1000000.0;
        cout << "Tempo di completamento: " << dt << " ms." << endl;

        double tuples_per_sec = atomic_load( &num_tuples_generated ) / ( dt / 1000.0 );
        cout << "Media elementi processati al secondo: " << tuples_per_sec 
        <<  endl;
    }
}
