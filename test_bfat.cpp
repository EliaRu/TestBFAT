#include <cstdint>
#include <iostream>
#include <string>

#include <getopt.h>

#include <windflow.hpp>
#include <windflow_gpu.hpp>

#include "common_gpu.hpp"

using namespace std;

size_t stream_len;
int64_t rate;
size_t win_len;
size_t slide;
size_t batch_len;

size_t num_keys = 1;
size_t num_workers = 1;
size_t num_sources = 1;
size_t extra = 0;
bool rebuildFAT = false;
bool useFF = false;
uint64_t quantum = 0;
bool fakeTS = false;
operator_t<tuple_t> functions = {
    SumLift, SumCombine, SumLower, true
};

template< typename Fun_t >
int RunPipe( Fun_t f )
{
    if( useFF ) {
       Win_FAT_GPU<tuple_t, tuple_t, Fun_t, tuple_t>* seq;
        if( fakeTS ) {
            seq = new Win_FAT_GPU<tuple_t, tuple_t, Fun_t, tuple_t>(
                functions.lift,
                f,
                win_len,
                slide,
                quantum,
                batch_len,
                rebuildFAT,
                "bfatfakets"
            );
        } else {
            seq = new Win_FAT_GPU<tuple_t, tuple_t, Fun_t, tuple_t>(
                functions.lift,
                f,
                win_len,
                slide,
                batch_len,
                rebuildFAT,
                "bfatfakets"
            );
        } 

        const int only_one = 1;
        Generator gen( stream_len, rate, num_keys );
        Source<tuple_t> source( 
            gen, 
            only_one, 
            "generator", 
            []( RuntimeContext&r ) {return;} 
        );

        Consumer consumer( num_keys, batch_len, functions );
        Sink<tuple_t> sink( 
            consumer, 
            only_one, 
            "sink", 
            []( RuntimeContext& r ) { return; } 
        );

        ff_pipeline pipe;
        pipe.add_stage(&source);
        pipe.add_stage(seq);
        pipe.add_stage(&sink);

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

        delete seq;
    } else {
        Key_Farm_GPU<tuple_t, tuple_t, Fun_t>* kf;
        if( fakeTS ) {
            kf = new Key_Farm_GPU<tuple_t, tuple_t, Fun_t>(
                    functions.lift, 
                    f,
                    win_len,
                    slide,
                    quantum,
                    batch_len,
                    rebuildFAT,
                    num_workers,
                    "bfatfakets"
            );
        } else {
            kf = new Key_Farm_GPU<tuple_t, tuple_t, Fun_t>(
                    functions.lift, 
                    f,
                    win_len,
                    slide,
                    batch_len,
                    rebuildFAT,
                    num_workers,
                    "bfat"
            );
        }

        Generator gen( stream_len, rate, num_keys );
        Source<tuple_t> source( 
            gen, 
            num_sources, 
            "generator", 
            []( RuntimeContext&r ) {return;} 
        );

        Consumer consumer( num_keys, batch_len, functions );
        Sink<tuple_t> sink( 
            consumer, 
            1, 
            "sink", 
            []( RuntimeContext& r ) { return; } 
        );

        MultiPipe pipe( "Test con Key Farm e Batched FAT" );
        pipe.add_source( source );
        pipe.add( *kf );
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
        delete kf;
    }
}

int main( int argc, char *argv[] )
{
    string op = "sum";

    int option = 0;
    int option_index = 0;
    int num_mandatory_parameters = 0;
    string cmd_message = argv[0];
    cmd_message += "-h -l <stream length> -r <rate> -w <win length> -s <win slide> -b <batch length> [-k <keys number>] [-p <workers number>] [--num-sources <sources number>] [--rebuild] [--operator <operator>] [--quantum <time>] [--use-ff]\n";
    while( 1 ) {
        static struct option long_options[] = {
            { "num-sources",    required_argument,  0, 0 },
            { "quantum",        required_argument,  0, 1 },
            { "rebuild",        no_argument,        0, 2 },
            { "operator",       required_argument,  0, 3 },
            { "use-ff",         no_argument,        0, 4 },
            { 0,                0,                  0, 0 }
        };

        option = getopt_long( argc, argv, "hl:r:w:s:b:k:p:", 
                long_options, &option_index );
        if( option == -1 ) {
            break;
        }
        switch( option ) {
            case 0:
                num_sources = atoi( optarg ); 
                break;
            case 1:
                quantum = strtoull( optarg, NULL, 10 );
                fakeTS = true;
                break;
            case 2:
                rebuildFAT = true;
                break;
            case 3:
                if( GetOperator( optarg, functions ) == -1 ) {
                    return -1;
                }
                op = optarg;
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
            case 'b':
                batch_len = atoi( optarg );
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
    if( num_mandatory_parameters != 5 ) {
        cout << cmd_message;
        return 0;
    }

    if( op == "count" ) {
        auto G = [] __host__ __device__ ( 
                size_t key, 
                uint64_t wid, 
                const tuple_t &a, 
                const tuple_t &b, 
                tuple_t &res ) 
        {
            res.key = key;
            res.id = wid;
            res.ts = a.ts > b.ts ? a.ts : b.ts;
            res.value = a.value + b.value;
            return 0;
        };
        return RunPipe<decltype( G )>( G );
    } else if( op == "sum" ) {
        auto G = [] __host__ __device__ ( 
                size_t key, 
                uint64_t wid, 
                const tuple_t &a, 
                const tuple_t &b, 
                tuple_t &res ) 
        {
            res.key = key;
            res.id = wid;
            res.ts = a.ts > b.ts ? a.ts : b.ts;
            res.value = a.value + b.value;
            return 0;
        };
        return RunPipe<decltype( G )>( G );
    } else if( op == "max" ) {
        auto G = [] __host__ __device__ ( 
                size_t key, 
                uint64_t wid, 
                const tuple_t &a, 
                const tuple_t &b, 
                tuple_t &res ) 
        {
            res.key = key;
            res.id = wid;
            res.ts = a.ts > b.ts ? a.ts : b.ts;
            res.value = a.value < b.value ? b.value : a.value;
            return 0;
        };
        return RunPipe<decltype( G )>( G );
    } else if( op == "arithmetic-mean" ) {
        auto G = [] __host__ __device__ ( 
                size_t key, 
                uint64_t wid, 
                const tuple_t &a, 
                const tuple_t &b, 
                tuple_t &res ) 
        {
            res.key = key;
            res.id = wid;
            res.ts = a.ts > b.ts ? a.ts : b.ts;
            res.n = a.n + b.n;
            res.value = a.value + b.value;
            return 0;
        };
        return RunPipe<decltype( G )>( G );
    } else if( op == "geometric-mean" ) {
        auto G = [] __host__ __device__ ( 
                size_t key, 
                uint64_t wid, 
                const tuple_t &a, 
                const tuple_t &b, 
                tuple_t &res ) 
        {
            res.key = key;
            res.id = wid;
            res.ts = a.ts > b.ts ? a.ts : b.ts;
            res.n = a.n + b.n;
            res.value = a.value * b.value;
            return 0;
        };
        return RunPipe<decltype( G )>( G );
    } else if( op == "max-count" ) {
        auto G = [] __host__ __device__ ( 
                size_t key, 
                uint64_t wid, 
                const tuple_t &a, 
                const tuple_t &b, 
                tuple_t &res ) 
        {
            res.key = key;
            res.id = wid;
            res.ts = a.ts > b.ts ? a.ts : b.ts;
            if( a.value > b.value ) {
                res.n = a.n;
                res.value = a.value;
            } else if( a.value == b.value ) {
                res.n = a.n + b.n;
                res.value = a.value;
            } else {
                res.n = b.n;
                res.value = b.value;
            }
            return 0;
        };
        return RunPipe<decltype( G )>( G );
    } else if( op == "sample-stddev" ) {
        auto G = [] __host__ __device__ ( 
                size_t key, 
                uint64_t wid, 
                const tuple_t &a, 
                const tuple_t &b, 
                tuple_t &res ) 
        {
            res.key = key;
            res.id = wid;
            res.ts = a.ts > b.ts ? a.ts : b.ts;
            res.n = a.n + b.n;
            res.value = a.value + b.value;
            res.secValue = a.secValue + b.secValue;
            return 0;
        };
        return RunPipe<decltype( G )>( G );
    } else if( op == "arg-max" ) {
        auto G = [] __host__ __device__ ( 
                size_t key, 
                uint64_t wid, 
                const tuple_t &a, 
                const tuple_t &b, 
                tuple_t &res ) 
        {
            res.key = key;
            res.id = wid;
            res.ts = a.ts > b.ts ? a.ts : b.ts;
            if( a.value > b.value ) {
                res.value = a.value;
                res.secValue = a.secValue;
            } else if( a.value == b.value ) {
                res.value = a.value;
                res.secValue = a.secValue;
            } else {
                res.value = b.value;
                res.secValue = b.secValue;
            }
            return 0;
        };
        return RunPipe<decltype( G )>( G );
    }
    return 0;
}
