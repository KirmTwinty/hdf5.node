#include <v8.h>
#include <node.h>

#include "h5_pt.hpp"

namespace NodeHDF5 {
  v8::Persistent<v8::Function> PacketTable::Constructor;
}
