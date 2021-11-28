#include "postgres.h"
#include "access/mixdest.h"
#include "executor/tqueue.h"

typedef struct MixDestReceiver
{
	DestReceiver pub;
    size_t partitions;
    size_t current_dest;
    DestReceiver *print;
	DestReceiver *tqueues[FLEXIBLE_ARRAY_MEMBER];
} MixDestReceiver;

static void MixDestStartUp(DestReceiver *self, int operation,
							 TupleDesc typeinfo);
static bool MixDestReceiveSlot(TupleTableSlot *slot, DestReceiver *self);
static void MixDestShutDown(DestReceiver *self);
static void MixDestDestroy(DestReceiver *self);

DestReceiver *
CreateMixDest(DestReceiver *dest, size_t partitions, shm_mq_handle **handles)
{
    MixDestReceiver *self;
    self = (MixDestReceiver*) palloc0(offsetof(MixDestReceiver, tqueues) + sizeof(TQueueDestReceiver *) * (partitions-1));

    self->pub.receiveSlot = MixDestReceiveSlot;
    self->pub.rStartup = MixDestStartUp;
    self->pub.rShutdown = MixDestShutDown;
    self->pub.rDestroy = MixDestDestroy;
    self->pub.mydest = dest->mydest; // align with print

    self->print = dest;
    self->partitions = partitions;
    self->current_dest = 0;

    for (size_t i = 0; i < partitions-1; ++i)
    {
        self->tqueues[i] = CreateTupleQueueDestReceiver(handles[i]);
    }
    return (DestReceiver *) self;
}

static void MixDestStartUp(DestReceiver *self, int operation, TupleDesc typeinfo)
{
    MixDestReceiver *myReceiver = (MixDestReceiver *) self;
    myReceiver->print->rStartup(myReceiver->print, operation, typeinfo);

    for (size_t i = 0; i < myReceiver->partitions-1; ++i)
    {
        myReceiver->tqueues[i]->rStartup(myReceiver->tqueues[i], operation, typeinfo);
    }
}

static bool MixDestReceiveSlot(TupleTableSlot *slot, DestReceiver *self)
{
    MixDestReceiver *myReceiver = (MixDestReceiver *) self;

    bool res = false;

    while (!res) // send clockwise until success or reach to print (print will always success)
    {
        if (myReceiver->current_dest == 0)
        {
            // elog(DEBUG1, "send to print");
            res = myReceiver->print->receiveSlot(slot, myReceiver->print);
            Assert(res);
        }
        else
        {
            size_t pos = myReceiver->current_dest-1;
            // elog(DEBUG1, "send to tqueue %d", pos);
            res = myReceiver->tqueues[pos]->receiveSlot(slot, myReceiver->tqueues[pos]);
            if(!res)
            {
                // elog(DEBUG1, "tqueue %d is not ready yet!", pos);
            }
        }
        myReceiver->current_dest = (myReceiver->current_dest + 1) % myReceiver->partitions; 
    }

    return true; 
}

static void MixDestShutDown(DestReceiver *self)
{
    MixDestReceiver *myReceiver = (MixDestReceiver *) self;
    myReceiver->print->rShutdown(myReceiver->print);
}

void MixDestShutDownTQueues(DestReceiver *self)
{
    MixDestReceiver *myReceiver = (MixDestReceiver *) self;

    for (size_t i = 0; i < myReceiver->partitions-1; ++i)
    {
        myReceiver->tqueues[i]->rShutdown(myReceiver->tqueues[i]);
    }
}

static void MixDestDestroy(DestReceiver *self)
{
    MixDestReceiver *myReceiver = (MixDestReceiver *) self;
    myReceiver->print->rDestroy(myReceiver->print);

    for (size_t i = 0; i < myReceiver->partitions-1; ++i)
    {
        myReceiver->tqueues[i]->rDestroy(myReceiver->tqueues[i]);
    }

    pfree(self);
}