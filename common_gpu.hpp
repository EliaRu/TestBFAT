
/* *****************************************************************************
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License version 3 as
 *  published by the Free Software Foundation.
 *  
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 *  License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 ******************************************************************************
 */

/*  
 *  Test Program of the Win_Seq Pattern (CB windows with an Incremental Query)
 *  
 *  Test program of the Win_Seq pattern instantiated with an incremental query.
 *  The query computes the sum of the value attribute of all the tuples in the window.
 *  The sliding window specification uses the count-based model.
 */ 

// includes
#include <string>
#include <iostream>
#include <cmath>
#include <cstdlib>
#include <experimental/optional>
#include <vector>
#include <cstdint>
#include <atomic>

//#include <windflow.hpp>
//#include <windflow_gpu.hpp>

// struct of the input tuple
struct tuple_t
{
    size_t key;
    uint64_t id;
    uint64_t ts;
    uint64_t n;
    int64_t value;
    int64_t secValue;
    bool isZero;
    tuple_t *pack;

    // constructor
    tuple_t(size_t _key, uint64_t _id, uint64_t _ts, int64_t _value)
    : key(_key), 
      id(_id), 
      ts(_ts), 
      n( 0 ), 
      value(_value), 
      secValue( 0 ), 
      isZero( false ),
      pack( nullptr )
    { }

    tuple_t(
        size_t _key, 
        uint64_t _id, 
        uint64_t _ts, 
        uint64_t _n, 
        int64_t _value, 
        int64_t _secValue 
    ) : key(_key), 
        id(_id), 
        ts(_ts), 
        n( _n ), 
        value(_value), 
        secValue( _secValue ), 
        isZero( false ),
        pack( nullptr )
    { }

    // default constructor
    tuple_t( ) 
    : key(0), 
      id(0), 
      ts(0), 
      n( 0 ), 
      value(0), 
      secValue( 0 ), 
      isZero( true ),
      pack( nullptr )
    {}

    tuple_t( const tuple_t& _t ) {
        key = _t.key;
        id = _t.id;
        ts = _t.ts;
        n = _t.n;
        value = _t.value;
        secValue = _t.secValue;
        isZero = _t.isZero;
        pack = _t.pack;
    }

    // destructor
    ~tuple_t() {
    }

    // getInfo method
    tuple<size_t, uint64_t, uint64_t> getControlFields() const
    {
        return tuple<size_t, uint64_t, uint64_t>(key, id, ts);
    }

    // setInfo method
    void setControlFields(size_t _key, uint64_t _id, uint64_t _ts)
    {
        key = _key;
        id = _id;
        ts = _ts;
    }
};

int CountLift(size_t key, uint64_t id, const tuple_t& t, tuple_t& res )
{
    res.key = t.key;
    res.id = t.id;
    res.ts = t.ts;
    res.value = t.isZero ? 0 : 1;
    res.isZero = false;
    return 0;
}

int CountCombine( size_t key, uint64_t id, const tuple_t& a, const tuple_t &b, tuple_t &res )
{
    res.key = key;
    res.id = id;
    res.ts = a.ts > b.ts ? a.ts : b.ts;
    res.value = a.value + b.value;
    return 0;
}

int CountLower( const tuple_t& t, tuple_t& res )
{
    res.key = t.key;
    res.id = t.id;
    res.ts = t.ts;
    res.value = t.value;
    return 0;
}

int SumLift(size_t key, uint64_t id, const tuple_t& t, tuple_t& res )
{
    res.key = t.key;
    res.id = t.id;
    res.ts = t.ts;
    res.value = t.value;
    res.isZero = false;
    return 0;
}

int SumCombine( size_t key, uint64_t id, const tuple_t& a, const tuple_t &b, tuple_t &res )
{
    res.key = key;
    res.id = id;
    res.ts = a.ts > b.ts ? a.ts : b.ts;
    res.value = a.value + b.value;
    return 0;
}

int SumLower( const tuple_t& t, tuple_t& res )
{
    res.key = t.key;
    res.id = t.id;
    res.ts = t.ts;
    res.value = t.value;
    return 0;
}

int MaxLift(size_t key, uint64_t id, const tuple_t& t, tuple_t& res )
{
    res.key = t.key;
    res.id = t.id;
    res.ts = t.ts;
    res.value = t.isZero ? INT64_MIN : t.value;
    res.isZero = false;
    return 0;
}

int MaxCombine( size_t key, uint64_t id, const tuple_t& a, const tuple_t &b, tuple_t &res )
{
    res.key = key;
    res.id = id;
    res.ts = a.ts > b.ts ? a.ts : b.ts;
    res.value = a.value < b.value ? b.value : a.value;
    return 0;
}

int MaxLower( const tuple_t& t, tuple_t& res )
{
    res.key = t.key;
    res.id = t.id;
    res.ts = t.ts;
    res.value = t.value;
    return 0;
}


int ArithmeticMeanLift(size_t key, uint64_t id, const tuple_t& t, tuple_t& res )
{
    res.key = t.key;
    res.id = t.id;
    res.ts = t.ts;
    res.n = t.isZero ? 0 : 1;
    res.value = t.isZero ? 0 : t.value;
    res.isZero = false;
    return 0;
}

int ArithmeticMeanCombine( size_t key, uint64_t id, const tuple_t& a, const tuple_t &b, tuple_t &res )
{
    res.key = key;
    res.id = id;
    res.ts = a.ts > b.ts ? a.ts : b.ts;
    res.n = a.n + b.n;
    res.value = a.value + b.value;
    return 0;
}

int ArithmeticMeanLower( const tuple_t& t, tuple_t& res )
{
    res.key = t.key;
    res.id = t.id;
    res.ts = t.ts;
    res.n = t.n;
    if( t.n != 0 ) {
        res.value = t.value / t.n;
    } else {
        res.value = 0;
    }
    return 0;
}


int GeometricMeanLift(size_t key, uint64_t id, const tuple_t& t, tuple_t& res )
{
    res.key = t.key;
    res.id = t.id;
    res.ts = t.ts;
    res.n = t.isZero ? 0 : 1;
    res.value = t.isZero ? 1 : t.value;
    res.isZero = false;
    return 0;
}

int GeometricMeanCombine( size_t key, uint64_t id, const tuple_t& a, const tuple_t &b, tuple_t &res )
{
    res.key = key;
    res.id = id;
    res.ts = a.ts > b.ts ? a.ts : b.ts;
    res.n = a.n + b.n;
    res.value = a.value * b.value;
    return 0;
}

int GeometricMeanLower( const tuple_t& t, tuple_t& res )
{
    res.key = t.key;
    res.id = t.id;
    res.ts = t.ts;
    res.n = t.n;
    if( t.n != 0 ) {
        res.value = pow( t.value, 1.0 / t.n );
    } else {
        res.value = 0;
    }
    return 0;
}

int MaxCountLift(size_t key, uint64_t id, const tuple_t& t, tuple_t& res )
{
    res.key = t.key;
    res.id = t.id;
    res.ts = t.ts;
    res.n = t.isZero ? 0 : 1;
    res.value = t.isZero ? INT64_MIN : t.value;
    res.isZero = false;
    return 0;
}

int MaxCountCombine( size_t key, uint64_t id, const tuple_t& a, const tuple_t &b, tuple_t &res )
{
    res.key = key;
    res.id = id;
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
}

int MaxCountLower( const tuple_t& t, tuple_t& res )
{
    res.key = t.key;
    res.id = t.id;
    res.ts = t.ts;
    res.value = t.n;
    return 0;
}

int SampleStdDevLift(size_t key, uint64_t id, const tuple_t& t, tuple_t& res )
{
    res.key = t.key;
    res.id = t.id;
    res.ts = t.ts;
    res.n = t.isZero ? 0 : 1;
    res.value = t.isZero ? 0 : t.value;
    res.secValue = t.isZero ? 0 : t.value * t.value;
    res.isZero = false;
    return 0;
}

int SampleStdDevCombine( size_t key, uint64_t id, const tuple_t& a, const tuple_t &b, tuple_t &res )
{
    res.key = key;
    res.id = id;
    res.ts = a.ts > b.ts ? a.ts : b.ts;
    res.n = a.n + b.n;
    res.value = a.value + b.value;
    res.secValue = a.secValue + b.secValue;
    return 0;
}

int SampleStdDevLower( const tuple_t& t, tuple_t& res )
{
    res.key = t.key;
    res.id = t.id;
    res.ts = t.ts;
    res.n = t.n;
    if( t.n != 0 && t.n != 1 ) {
        res.value = sqrt( ( 1.0 / ( t.n - 1 ) ) * ( t.secValue - t.value * t.value / t.n ) );
    } else {
        res.value = 0;
    }
    return 0;
}

int ArgMaxLift(size_t key, uint64_t id, const tuple_t& t, tuple_t& res )
{
    res.key = t.key;
    res.id = t.id;
    res.ts = t.ts;
    res.value = t.isZero ? INT64_MIN : t.value;
    res.secValue = t.isZero ? -1 : t.secValue;
    res.isZero = false;
    return 0;
}

int ArgMaxCombine( size_t key, uint64_t id, const tuple_t& a, const tuple_t &b, tuple_t &res )
{
    res.key = key;
    res.id = id;
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
}

int ArgMaxLower( const tuple_t& t, tuple_t& res )
{
    res.key = t.key;
    res.id = t.id;
    res.ts = t.ts;
    res.value = t.secValue;
    return 0;
}

template <typename T>
struct operator_t {
    function<int( size_t, uint64_t, const T&, T&)> lift;
    function<int( size_t, uint64_t, const T&, const T&, T&)> combine;
    function<int( const T&, T&)> lower;
    bool isCommutative;
};

int GetOperator( const string& op, operator_t<tuple_t>& functions )
{
    if( op == "count" ) {
        functions.lift = CountLift;
        functions.combine = CountCombine;
        functions.lower = CountLower;
        functions.isCommutative = true;
    } else if( op == "sum" ) {
        functions.lift = SumLift;
        functions.combine = SumCombine;
        functions.lower = SumLower;
        functions.isCommutative = true;
    } else if( op == "max" ) {
        functions.lift = MaxLift;
        functions.combine = MaxCombine;
        functions.lower = MaxLower;
        functions.isCommutative = true;
    } else if( op == "arithmetic-mean" ) {
        functions.lift = ArithmeticMeanLift;
        functions.combine = ArithmeticMeanCombine;
        functions.lower = ArithmeticMeanLower;
        functions.isCommutative = true;
    } else if( op == "geometric-mean" ) {
        functions.lift = GeometricMeanLift;
        functions.combine = GeometricMeanCombine;
        functions.lower = GeometricMeanLower;
        functions.isCommutative = true;
    } else if( op == "max-count" ) {
        functions.lift = MaxCountLift;
        functions.combine = MaxCountCombine;
        functions.lower = MaxCountLower;
        functions.isCommutative = true;
    } else if( op == "sample-stddev" ) {
        functions.lift = SampleStdDevLift;
        functions.combine = SampleStdDevCombine;
        functions.lower = SampleStdDevLower;
        functions.isCommutative = true;
    } else if( op == "arg-max" ) {
        functions.lift = ArgMaxLift;
        functions.combine = ArgMaxCombine;
        functions.lower = ArgMaxLower;
        functions.isCommutative = false;
    } else {
        return -1;
    }
    return 0;
}

std::atomic<uint64_t> num_tuples_generated( 0 );

// class Generator: first stage that produces a stream of integers
class Generator
{
private:
    size_t len; // stream length per key
    int64_t rate; // number of keys
    size_t numKeys;
    uint64_t waiting_time;
    int pad;
    std::vector<tuple_t> vt;
    bool bandwith_test;

    volatile unsigned long t0;
    unsigned long base_time;

    const uint64_t sec = 1000000000.0;

    size_t i;
    size_t j;

public:
    // constructor
    Generator(size_t _len, int64_t _rate, size_t _numKeys )
    : len(_len ), 
      rate( _rate ), 
      pad( 0 ), 
      i( 0 ), 
      j( 0 ), 
      numKeys( _numKeys )
    {
        if( rate < 0 ) {
            waiting_time = -rate * sec;
            bandwith_test = true;
        } else {
            waiting_time = sec / rate;
            bandwith_test = false;
        }
        t0 = current_time_nsecs( );
    }

    // destructor
    ~Generator() {}

    bool operator( )( tuple_t &t ) {
        if( !bandwith_test ) {
            volatile unsigned long t1;
            while( true ) {
                t1 = current_time_nsecs( );
                if( t1 - t0 >= waiting_time ) {
                    break;
                }
            }
            if( i == len - 1 && j == numKeys - 1 ) {
                t.key = j;
                t.id = i;
                t.ts = t1;
                t.value = 1;
                t.secValue = i;
                t.isZero = false;
                num_tuples_generated++;
                return false;
            } else {
                t.key = j;
                t.id = i;
                t.ts = t1;
                t.value = 1;
                t.secValue = i;
                t.isZero = false;
                j++;
                if( j == numKeys ) {
                    i++;
                    j = 0;
                }
                t0 = current_time_nsecs( );
                num_tuples_generated++;
                return true;
            }
        } else {
            volatile unsigned long t1 = current_time_nsecs( );
            if( t1 - t0 >= waiting_time ) {
                t.key = j;
                t.id = i;
                t.ts = t1;
                t.value = 1;
                t.secValue = i;
                t.isZero = false;
                j++;
                if( j == numKeys ) {
                    i++;
                    j = 0;
                }
                num_tuples_generated++;
                return false;
            } else {
                t.key = j;
                t.id = i;
                t.ts = t1;
                t.value = 1;
                t.secValue = i;
                t.isZero = false;
                j++;
                if( j == numKeys ) {
                    i++;
                    j = 0;
                }
                num_tuples_generated++;
                return true;
            }
        }
    }
};

// class Consumer: last stage that prints the query results
class Consumer
{
private:
    size_t received; // counter of received results
    size_t noInBatch;
    uint64_t latency;
    unsigned long totalsum;
    size_t keys;
    size_t batch_len;
    std::vector<size_t> check_counters;
    unsigned long t0;
    bool isFirstTuple;
    operator_t<tuple_t> functions;
    tuple_t acc;

public:
    // constructor
    Consumer(size_t _keys, size_t _batch_len, operator_t<tuple_t>& _functions) : 
        received(0),
        noInBatch( 0 ),
        latency( 0 ),
        totalsum(0), 
        keys(_keys), 
        batch_len( _batch_len ),
        check_counters( _keys, 0 ),
        isFirstTuple( true ),
        functions( _functions )
    {
        functions.lift( 0, 0, acc, acc );
    }

    // destructor
    ~Consumer()
    {
    }

    void processTuple( unsigned long t1, const tuple_t& t ) {
        received++;
        noInBatch++;
        functions.combine( 0, 0, acc, t, acc );
        latency += ( t1 - t.ts );
        //check_counters[t.key]++;
    }

    void operator( )( std::experimental::optional<tuple_t> &pout ) {
        if( pout ) {
            if( isFirstTuple ) {
                t0 = current_time_nsecs( );
                isFirstTuple = false;
            }

            auto t1 = current_time_nsecs( );
            if( pout->pack == nullptr ) {
                processTuple( t1, *pout );
            } else {
                for( size_t i = 0; i < batch_len; i++ ) {
                    processTuple( t1, ( pout->pack )[i] );
                }
                delete[] pout->pack;
            }
        } else {
            auto t1 = current_time_nsecs( );
            auto elapsed_time = ( t1 - t0 ) / 1000000000.0;

            double meanLatency = ( double ) latency / received;
            cout << "Mean latency in ms: " << meanLatency / 1000000.0 
            << endl;

            tuple_t res;
            functions.lower( acc, res );
            cout << "Received " << received << " window results, "
            << "final result: " << res.value << endl;
        }
/*
    void operator( )( std::experimental::optional<tuple_t> &pout ) {
        if( pout ) {
            if( isFirstTuple ) {
                t0 = current_time_nsecs( );
                isFirstTuple = false;
            }

            auto t1 = current_time_nsecs( );
            if( pout->pack == nullptr ) {
                processTuple( t1, *pout );
            } else {
                for( size_t i = 0; i < batch_len; i++ ) {
                    processTuple( t1, ( pout->pack )[i] );
                }
                delete[] pout->pack;
            }

            auto elapsed_time = t1 - t0;

            if( elapsed_time >= 1000000000L ) {
                cout << "Received " << noInBatch << " window results " 
                << "in the last " << elapsed_time / 1000000.0
                << " ms. ";

                double meanLatency = ( double ) latency / noInBatch;
                cout << "Mean latency: " << meanLatency / 1000000.0 
                << " ms." << endl;

                elapsed_time = 0L;
                t0 = current_time_nsecs( );
                noInBatch = 0;
                latency = 0;
            }
        } else {
            auto t1 = current_time_nsecs( );
            auto elapsed_time = t1 - t0;

            cout << "Received " << noInBatch << " window results " 
            << "in the last " << elapsed_time / 1000000.0
            << " ms. ";

            double meanLatency = ( double ) latency / noInBatch;
            cout << "Mean latency: " << meanLatency / 1000000.0 
            << " ms." << endl;

            tuple_t res;
            functions.lower( acc, res );
            cout << "Received " << received << " window results, "
            << "final result: " << res.value << endl;
        }
*/
    }

    // method to get the total sum of the windows
    unsigned long getTotalSum() const  {
        return totalsum;
    }
};
