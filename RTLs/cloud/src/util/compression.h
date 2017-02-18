//===-------- compression.h----- - Information about Target RTLs ------ C++ -*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
// Util function
//
//===----------------------------------------------------------------------===//

#ifndef _INCLUDE_COMPRESSION_H_
#define _INCLUDE_COMPRESSION_H_


int decompress_file(std::string comp_file, char *ptr);

int compress_to_file(std::string comp_file, char *ptr, int size);

#endif
