#ifndef MIXDEST_H
#define MIXDEST_H

#include "utils/portal.h"
#include "storage/shm_mq.h"
#include "tcop/dest.h"

extern DestReceiver *CreateMixDest(DestReceiver *dest, size_t partitions, shm_mq_handle **handles);
extern void MixDestShutDownTQueues(DestReceiver *self);

#endif /*MIXDEST_H*/