/*
 * config.h
 *
 *  Created on: 2011-1-5
 *      Author: saint
 */



#ifndef CONFIG_H_
#define CONFIG_H_
#include <stdbool.h>

typedef struct _config
{
	char *logdir;	
	int task_group_size;
	int maxconns;
	int maxidle;
	bool useketama;
	int port;
	bool daemon;
	bool verbose_mode;
	char *host_ip;

} s_config;


s_config * glob_config ;
void init_config(const char *config_file);
void config_free();

#endif /* CONFIG_H_ */
