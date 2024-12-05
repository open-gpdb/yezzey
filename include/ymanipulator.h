#pragma once
#include <stdint.h>

class YTrashManipulator {
public:
  virtual ~YTrashManipulator(){

  };
  
  virtual bool close() = 0;
  bool collect_obsolete_chunks();
  bool delete_obsolete_chunks();
};

