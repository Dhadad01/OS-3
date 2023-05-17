#include "pdebug.hpp"

#ifndef UNUSED
#define UNUSED(x) ((void)x)
#endif // UNUSED

#include <cstdarg>

#ifdef DEBUG

#include <cstdio>
#include <string>

static const std::string DEBUG_PREFIX = "DEBUG: ";

void
pdebug(const char* format, ...)
{
  std::va_list args;
  va_start(args, format);

  //(void)std::printf(DEBUG_PREFIX.c_str());
  (void)std::vprintf(format, args);
  (void)std::fflush(stdout);

  va_end(args);
}

#else // no DEBUG

void
pdebug(const char* format, ...)
{
  UNUSED(format);
}

#endif // DEBUG
