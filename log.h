#ifndef __LOG__
#define __LOG__

#define 	L_DEBUG                 1
#define 	L_INFO                  2
#define 	L_WARN                  3
#define 	L_ERR                   4
#define 	L_CONS                  128

#define     DEBUG_COMMON_FLAG       2
#define     DEBUG_CACHE_FLAG        3

extern int debug_flag;
#ifdef DEBUG
/* add -DPRINTDEBUG to CPPFLAGS in Makefile for debug outputs */
#define DEBUG(...)                                 \
  do {                                             \
	log_debug("%s() @ %d: ", __FUNCTION__, __LINE__); \
	log_debug(__VA_ARGS__);                           \
  } while (0)
#else
#define DEBUG(...)
#endif
//#define 	DEBUG                   if ( debug_flag == DEBUG_COMMON_FLAG ) log_debug
#define     CACHE_DEBUG             if ( debug_flag >= DEBUG_CACHE_FLAG ) log_debug

void    log_init(const char *log_dir, int log_flag, const char *log_progname);
int     jlog(int lvl, char *msg, ...);
int     log_debug(char *msg, ...);
void    log_destroy();

#endif
