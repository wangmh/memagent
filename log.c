#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "log.h"

/* logging */
typedef struct log_st
{
    FILE            *file;      /* ��־����ļ������� */
    char            *fileName;  /* ��־����ļ���� */
    char            *filePath;  /* ��־����ļ�����Ŀ¼ */
    int             isDebug;
    size_t          maxSize;    /* ��־�ļ�����С */
    size_t          actSize;    /* ��־�ļ�ʵ�ʴ�С */
    pthread_mutex_t mutex;      /* ��־�ļ��� */
} *log_t;

static const unsigned long  dftFileSize = (1 * 0x400 * 0x100000L);		// 1G

static pthread_mutex_t logMutex = PTHREAD_MUTEX_INITIALIZER;

#define MAX_LOG_LINE (4096)

static log_t sLogger = NULL;

void log_init(const char *log_dir, int log_flag, const char *log_progname)
{
	char ident[MAX_LOG_LINE];
	struct stat st;
    
    sLogger = (log_t) malloc(sizeof(struct log_st));
    memset(sLogger, 0, sizeof(struct log_st));

	memset((void *)ident, 0, sizeof(ident));
	snprintf(ident, MAX_LOG_LINE - 1 , "%.200s/%s.log", log_dir, log_progname);

    sLogger->file = fopen(ident, "a+");
    if(sLogger->file == NULL) {
        fprintf(stderr,
            "ERROR: couldn't open logfile: %m\n");
        return ;
    }

    sLogger->isDebug = log_flag;

    sLogger->fileName = strdup(ident);
    sLogger->filePath = strdup(log_dir);
    sLogger->maxSize = dftFileSize;

	//��ȡ�ļ���ԭʼ��С
    if (stat(ident, &st) == -1) {
        sLogger->actSize = 0;
	} else {
		sLogger->actSize = st.st_size;
	}

}

void log_destroy() {
    if (sLogger) {
        if (sLogger->fileName)
            free(sLogger->fileName);
        if (sLogger->filePath)
            free(sLogger->filePath);
        if (sLogger->file)
            fclose(sLogger->file);
        free(sLogger);
        sLogger = NULL;
    }
}

static void log_check(size_t size)
{
    if (sLogger->actSize + size > sLogger->maxSize)
    {
        char divFileName[512];
        struct timeval tmval;
        struct tm *time;
    
        gettimeofday(&tmval, NULL);
        time = localtime(&(tmval.tv_sec));
        // 2007-12-25 01:45:32:123456
        snprintf(divFileName, 512, "%s_%4d_%02d_%02d_%02d_%02d_%02d",
            sLogger->fileName, time->tm_year + 1900, time->tm_mon + 1,
            time->tm_mday, time->tm_hour, time->tm_min, time->tm_sec);

        fclose(sLogger->file);
        rename(sLogger->fileName, divFileName);
        sLogger->file = fopen(sLogger->fileName, "a+");
        sLogger->actSize = 0;
    }
}

/*
 *	Log the message to the logfile. Include the severity and
 *	a time stamp.
 */
static int do_log(int lvl, char *fmt, va_list ap)
{
    char *s = ": ";
    char message[MAX_LOG_LINE];
    time_t timeval;
    size_t len, size;

	if (lvl < sLogger->isDebug)
	{
		  return 0;
	}
    timeval = time(NULL);
    strcpy(message, ctime(&timeval));
    switch(lvl) {
        case L_DEBUG:
            s = ": Debug: ";
            break;
        case L_WARN:
            s = ": Warn: ";
            break;
        case L_INFO:
            s = ": Info: ";
            break;
        case L_ERR:
            s = ": Error: ";
            break;
    }

    strncpy(message + 24, s, sizeof(message) - 24);
    len = strlen(message);

    vsnprintf(message + len, MAX_LOG_LINE - len, fmt, ap);
    if (strlen(message) >= sizeof(message))
        /* What else can we do if we don't have vnsprintf */
        exit(-1);

    /*
     * Filter out characters not in Latin-1.
     */
    for (s = message; *s; s++) {
        if (*s == '\r' || *s == '\n') {
            *s = ' ';
        } else if ((unsigned char)(*s) < 32
                || ((unsigned char)(*s) >= 128
                    && (unsigned char)(*s) <= 160)) {
            *s = '?';
        }
    }

    size = strlen(message)+1;
    log_check(size);
    sLogger->actSize += size;
    fprintf(sLogger->file,"%s", message);
    fprintf(sLogger->file, "\n");
    fflush(sLogger->file);

    return 0;
}

int log_debug(char *msg, ...)
{
    va_list ap;
    int r;

    va_start(ap, msg);
    pthread_mutex_lock(&logMutex);
    r = do_log(L_DEBUG, msg, ap);
    pthread_mutex_unlock(&logMutex);
    va_end(ap);

    return r;
}

int jlog(int lvl, char *msg, ...)
{
    va_list ap;
    int r;

    va_start(ap, msg);
    pthread_mutex_lock(&logMutex);
    r = do_log(lvl, msg, ap);
    pthread_mutex_unlock(&logMutex);
    va_end(ap);

    return r;
}

