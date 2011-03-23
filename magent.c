/*

 Copyright (c) 2008, 2010 QUE Hongyu
 All rights reserved.

 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions
 are met:
 1. Redistributions of source code must retain the above copyright
 notice, this list of conditions and the following disclaimer.
 2. Redistributions in binary form must reproduce the above copyright
 notice, this list of conditions and the following disclaimer in the
 documentation and/or other materials provided with the distribution.

 THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
 FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 SUCH DAMAGE.
 */

/* Changelog:
 * 2008-08-20, coding started
 * 2008-09-04, v0.1 finished
 * 2008-09-07, v0.2 finished, code cleanup, drive_get_server function
 * 2008-09-09, support get/gets' multi keys(can > 7 keys)
 * 2008-09-10, ketama allocation
 * 2008-09-12, backup server added
 * 2008-09-12, try backup server for get/gets command
 * 2008-09-16, v0.3 finished
 * 2008-09-20, support unix domain socket
 * 2008-09-23, write "END\r\n" with the last packet of GET/GETS response
 * 2008-09-23, combine drive_get_server with drive_server functions -> drive_memcached_server function
 * 2008-10-05, fix header file include under BSD systems
 * 2011-02-11, add config file and log
 */

#define _GNU_SOURCE
#include <sys/types.h>

#if defined(__FreeBSD__)
#include <sys/uio.h>
#include <limits.h>
#else
#include <getopt.h>
#endif
#include <glib.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <netinet/in.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <sys/un.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <fcntl.h>
#include <time.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <event.h>

#include "log.h"
#include "config.h"
#include "ketama.h"

#define VERSION "0.7"

#define OUTOFCONN "SERVER_ERROR OUT OF CONNECTION"

#define BUFFERLEN 2048
#define MAX_TOKENS 8
#define COMMAND_TOKEN 0
#define KEY_TOKEN 1
#define BYTES_TOKEN 4
#define KEY_MAX_LENGTH 250
#define BUFFER_PIECE_SIZE 16

#define UNUSED(x) ( (void)(x) )
#define STEP 5

/* structure definitions */

static char *config_file;
typedef struct conn conn;
typedef struct matrix matrix;
typedef struct list list;
typedef struct buffer buffer;
typedef struct server server;

typedef enum {
	CLIENT_COMMAND, CLIENT_NREAD, /* MORE CLIENT DATA */
	CLIENT_TRANSCATION
} client_state_t;

typedef enum {
	SERVER_INIT, SERVER_CONNECTING, SERVER_CONNECTED, SERVER_ERROR
} server_state_t;

struct buffer {
	char *ptr;

	size_t used;
	size_t size;
	size_t len; /* ptr length */

	struct buffer *next;
};

/* list to buffers */
struct list {
	buffer *first;
	buffer *last;
};

/* connection to memcached server */
struct server {
	int sfd;
	server_state_t state;
	struct event ev;
	int ev_flags;

	matrix *owner;

	/* first response line
	 * NOT_FOUND\r\n
	 * STORED\r\n
	 */
	char line[BUFFERLEN];
	int pos;

	/* get/gets key ....
	 * VALUE <key> <flags> <bytes> [<cas unique>]\r\n
	 */
	int valuebytes;
	int has_response_header :1;
	int remove_trail :1;

	/* input buffer */
	list *request;
	/* output buffer */
	list *response;

	int pool_idx;
};

struct conn {
	/* client part */
	int cfd;
	client_state_t state;
	struct event ev;
	int ev_flags;

	/* command buffer */
	char line[BUFFERLEN + 1];
	int pos;

	int storebytes; /* bytes stored by CAS/SET/ADD/... command */

	struct flag {
		unsigned int is_get_cmd :1;
		unsigned int is_gets_cmd :1;
		unsigned int is_set_cmd :1;
		unsigned int is_incr_decr_cmd :1;
		unsigned int no_reply :1;
		unsigned int is_update_cmd :1;
		unsigned int is_backup :1;
		unsigned int is_last_key :1;
	} flag;

	int keycount; /* GET/GETS multi keys */
	int keyidx;
	char **keys;

	/* input buffer */
	list *request;
	/* output buffer */
	list *response;

	struct server *srv;
};

/* memcached server structure */
struct matrix {
	char *ip;
	int port;
	struct sockaddr_in dstaddr;
	char *name;
	int size;
	int used;
	struct server **pool;
};

typedef struct token_s {
	char *value;
	size_t length;
} token_t;

/* static variables */
static int curconns = 0, sockfd = -1;
static struct event ev_master;
static struct event signal_int;
static struct event signal_term;
static struct event signal_hup;

static struct matrix *matrixs = NULL; /* memcached server list */
static struct matrix *matrixs_new = NULL;
static int matrixcnt = 0;
static struct ketama *ketama = NULL;

static struct matrix *backups = NULL; /* backup memcached server list */
static int backupcnt = 0;
static struct ketama *backupkt = NULL;

//static char *socketpath = NULL;
//static int unixfd = -1;
//static struct event ev_unix;
const char *LOG_PROGRAMME = "magent";

//static struct event ev_timer;
time_t cur_ts;
char cur_ts_str[128];

static void free_matrix(matrix *m);
static void drive_client(const int, const short, void *);
static void drive_backup_server(const int, const short, void *);
static void drive_memcached_server(const int, const short, void *);
static void finish_transcation(conn *);
static void do_transcation(conn *);
static void start_magent_transcation(conn *);
static void out_string(conn *, const char *);
static void process_update_response(conn *);
static void process_get_response(conn *, int);
static void append_buffer_to_list(list *, buffer *);
static void try_backup_server(conn *);

static volatile sig_atomic_t signal_shutdown = 0;
static volatile sig_atomic_t signal_reload = 0;

/* the famous DJB hash function for strings from stat_cache.c*/
static int hashme(char *str) {
	unsigned int hash = 5381;
	const char *s;

	if (str == NULL)
		return 0;

	for (s = str; *s; s++) {
		hash = ((hash << 5) + hash) + *s;
	}
	hash &= 0x7FFFFFFF; /* strip the highest bit */
	return hash;
}

static buffer *
buffer_init_size(int size) {
	buffer *b;

	if (size <= 0)
		return NULL;
	b = (struct buffer *) calloc(sizeof(struct buffer), 1);
	if (b == NULL)
		return NULL;

	size += BUFFER_PIECE_SIZE - (size % BUFFER_PIECE_SIZE);

	b->ptr = (char *) calloc(1, size);
	if (b->ptr == NULL) {
		free(b);
		return NULL;
	}

	b->len = size;
	return b;
}

static void buffer_free(buffer *b) {
	if (!b)
		return;

	free(b->ptr);
	free(b);
}

static list *
list_init(void) {
	list *l;

	l = (struct list *) calloc(sizeof(struct list), 1);
	return l;
}

static void list_free(list *l, int keep_list) {
	buffer *b, *n;

	if (l == NULL)
		return;

	b = l->first;
	while (b) {
		n = b->next;
		buffer_free(b);
		b = n;
	}

	if (keep_list)
		l->first = l->last = NULL;
	else
		free(l);
}

static void remove_finished_buffers(list *l) {
	buffer *n, *b;

	if (l == NULL)
		return;
	b = l->first;
	while (b) {
		if (b->used < b->size) /* incompleted buffer */
			break;
		n = b->next;
		buffer_free(b);
		b = n;
	}

	if (b == NULL) {
		l->first = l->last = NULL;
	} else {
		l->first = b;
	}
}

static void copy_list(list *src, list *dst) {
	buffer *b, *r;
	int size = 0;

	if (src == NULL || dst == NULL || src->first == NULL)
		return;

	b = src->first;
	while (b) {
		size += b->size;
		b = b->next;
	}

	if (size == 0)
		return;

	r = buffer_init_size(size + 1);
	if (r == NULL)
		return;

	b = src->first;
	while (b) {
		if (b->size > 0) {
			memcpy(r->ptr + r->size, b->ptr, b->size);
			r->size += b->size;
		}
		b = b->next;
	}
	append_buffer_to_list(dst, r);
}

static void move_list(list *src, list *dst) {
	if (src == NULL || dst == NULL || src->first == NULL)
		return;

	if (dst->first == NULL)
		dst->first = src->first;
	else
		dst->last->next = src->first;

	dst->last = src->last;

	src->last = src->first = NULL;
}

static void append_buffer_to_list(list *l, buffer *b) {
	if (l == NULL || b == NULL)
		return;

	if (l->first == NULL) {
		l->first = l->last = b;
	} else {
		l->last->next = b;
		l->last = b;
	}
}

/* return -1 if not found
 * return pos if found
 */
static int memstr(char *s, char *find, int srclen, int findlen) {
	char *bp, *sp;
	int len = 0, success = 0;

	if (findlen == 0 || srclen < findlen)
		return -1;
	for (len = 0; len <= (srclen - findlen); len++) {
		if (s[len] == find[0]) {
			bp = s + len;
			sp = find;
			do {
				if (!*sp) {
					success = 1;
					break;
				}
			} while (*bp++ == *sp++);
			if (success)
				break;
		}
	}

	if (success)
		return len;
	else
		return -1;
}

static size_t tokenize_command(char *command, token_t *tokens,
		const size_t max_tokens) {
	char *s, *e;
	size_t ntokens = 0;

	if (command == NULL || tokens == NULL || max_tokens < 1)
		return 0;

	for (s = e = command; ntokens < max_tokens - 1; ++e) {
		if (*e == ' ') {
			if (s != e) {
				tokens[ntokens].value = s;
				tokens[ntokens].length = e - s;
				ntokens++;
				*e = '\0';
			}
			s = e + 1;
		} else if (*e == '\0') {
			if (s != e) {
				tokens[ntokens].value = s;
				tokens[ntokens].length = e - s;
				ntokens++;
			}

			break; /* string end */
		}
	}

	/*
	 * If we scanned the whole string, the terminal value pointer is null,
	 * otherwise it is the first unprocessed character.
	 */
	tokens[ntokens].value = *e == '\0' ? NULL : e;
	tokens[ntokens].length = 0;
	ntokens++;

	return ntokens;
}

static void server_free(struct server *s) {
	if (s == NULL)
		return;

	if (s->sfd > 0) {
		event_del(&(s->ev));
		close(s->sfd);
	}

	list_free(s->request, 0);
	list_free(s->response, 0);
	free(s);
}

static void pool_server_handler(const int fd, const short which, void *arg) {
	struct server *s;
	struct matrix *m;
	char buf[128];
	int toread = 0, toexit = 0, i;

	if (arg == NULL)
		return;
	s = (struct server *) arg;

	if (!(which & EV_READ))
		return;

	/* get the byte counts of read */
	if (ioctl(s->sfd, FIONREAD, &toread) || toread == 0) {
		toexit = 1;
	} else {
		if (toread > 128)
			toread = 128;

		if (0 == read(s->sfd, buf, toread))
			toexit = 1;
	}

	if (toexit) {
		if (glob_config->verbose_mode)
			fprintf(stderr, "%s: (%s.%d) CLOSE POOL SERVER FD %d\n",
					cur_ts_str, __FILE__, __LINE__, s->sfd);
		event_del(&(s->ev));
		close(s->sfd);

		list_free(s->request, 0);
		list_free(s->response, 0);
		m = s->owner;
		if (m) {
			if (s->pool_idx <= 0) {
				fprintf(stderr, "%s: (%s.%d) POOL SERVER FD %d, IDX %d <= 0\n",
						cur_ts_str, __FILE__, __LINE__, s->sfd, s->pool_idx);
			} else {
				/* remove from list */
				for (i = s->pool_idx; i < m->used; i++) {
					m->pool[i - 1] = m->pool[i];
					m->pool[i - 1]->pool_idx = i;
				}
				--m->used;
			}
		}
		free(s);
	}
}

/* put server connection into keep alive pool */
static void put_server_into_pool(struct server *s) {
	struct matrix *m;
	struct server **p;

	if (s == NULL)
		return;

	if (s->owner == NULL || s->state != SERVER_CONNECTED || s->sfd <= 0) {
		server_free(s);
		return;
	}

	list_free(s->request, 1);
	list_free(s->response, 1);
	s->pos = s->has_response_header = s->remove_trail = 0;

	m = s->owner;
	if (m->size == 0) {
		m->pool = (struct server **) calloc(sizeof(struct server *), STEP);
		if (m->pool == NULL) {
			fprintf(stderr, "%s: (%s.%d) out of memory for pool allocation\n",
					cur_ts_str, __FILE__, __LINE__);
			m = NULL;
		} else {
			m->size = STEP;
			m->used = 0;
		}
	} else if (m->used == m->size) {
		if (m->size < glob_config->maxidle) {
			p = (struct server **) realloc(m->pool, sizeof(struct server *)
					* (m->size + STEP));
			if (p == NULL) {
				fprintf(stderr,
						"%s: (%s.%d) out of memory for pool reallocation\n",
						cur_ts_str, __FILE__, __LINE__);
				m = NULL;
			} else {
				m->pool = p;
				m->size += STEP;
			}
		} else {
			m = NULL;
		}
	}

	if (m != NULL) {
		if (glob_config->verbose_mode)
			fprintf(stderr, "%s: (%s.%d) PUT SERVER FD %d -> POOL\n",
					cur_ts_str, __FILE__, __LINE__, s->sfd);
		m->pool[m->used++] = s;
		s->pool_idx = m->used;
		event_del(&(s->ev));

		event_set(&(s->ev), s->sfd, EV_READ | EV_PERSIST, pool_server_handler,
				(void *) s);
		event_add(&(s->ev), 0);
	} else {
		server_free(s);
	}

}

static GKeyFile *
load_config(const char *file) {
	GError *err = NULL;
	GKeyFile *keyfile;
	keyfile = g_key_file_new();
	g_key_file_set_list_separator(keyfile, ',');
	if (!g_key_file_load_from_file(keyfile, file, 0, &err)) {
		fprintf(stderr, "Parsing %s failed: %s", file, err->message);
		g_error_free(err);
		g_key_file_free(keyfile);
		return NULL;
	}
	return keyfile;
}

static void init_matrixs_item(char *optarg, struct matrix *m) {
	char *p;
	if (optarg == NULL || m == NULL)
		return;
	m->name = strdup(optarg);
	p = strchr(optarg, ':');
	if (p == NULL) {
		m->ip = strdup(optarg);
		m->port = 11211;
	} else {
		*p = '\0';
		m->ip = strdup(optarg);
		*p = ':';
		p++;
		m->port = atoi(p);
		if (m->port <= 0)
			m->port = 11211;
	}
	m->dstaddr.sin_family = AF_INET;
	m->dstaddr.sin_addr.s_addr = inet_addr(m->ip);
	m->dstaddr.sin_port = htons(m->port);
	m->used = 0;
	m->size = 0;
	m->pool = NULL;
	return;
}
static void add_backup_memcached(char *optarg) {
	struct matrix *m;

	if (backupcnt == 0) {
		backups = (struct matrix *) calloc(sizeof(struct matrix), 1);
		if (backups == NULL) {
			fprintf(stderr, "out of memory for %s\n", optarg);
			exit(1);
		}
		m = backups;
		backupcnt = 1;
	} else {
		backups = (struct matrix *) realloc(backups, sizeof(struct matrix)
				* (backupcnt + 1));
		if (backups == NULL) {
			fprintf(stderr, "out of memory for %s\n", optarg);
			exit(1);
		}
		m = backups + backupcnt;
		backupcnt++;
	}

	init_matrixs_item(optarg, m);

}

static void add_master_memcached(char *optarg) {
	struct matrix *m;
	if (matrixcnt == 0) {
		matrixs = (struct matrix *) calloc(sizeof(struct matrix), 1);
		if (matrixs == NULL) {
			fprintf(stderr, "out of memory for %s\n", optarg);
			exit(1);
		}
		m = matrixs;
		matrixcnt = 1;
	} else {
		matrixs = (struct matrix *) realloc(matrixs, sizeof(struct matrix)
				* (matrixcnt + 1));
		if (matrixs == NULL) {
			fprintf(stderr, "out of memory for %s\n", optarg);
			exit(1);
		}
		m = matrixs + matrixcnt;
		matrixcnt++;
	}
	init_matrixs_item(optarg, m);
}

static void init_general_config(GKeyFile *keyfile) {

	GError *err = NULL;
	glob_config->daemon = g_key_file_get_boolean(keyfile, "General",
			"daemon_mode", &err);
	if (err) {
		glob_config->daemon = true;
		g_clear_error(&err);
	}
	glob_config->logdir = g_key_file_get_string(keyfile, "General", "logdir",
			&err);
	if (err) {
		glob_config->logdir = "/home";
		g_clear_error(&err);
	}
	glob_config->verbose_mode = g_key_file_get_boolean(keyfile, "General",
			"verbose_mode", &err);
	if (err) {
		glob_config->verbose_mode = false;
		g_clear_error(&err);
	}
	glob_config->useketama = g_key_file_get_boolean(keyfile, "General",
			"useketama", &err);
	if (err) {
		glob_config->useketama = false;
		g_clear_error(&err);
	}
	glob_config->port
			= g_key_file_get_integer(keyfile, "General", "port", &err);
	if (err) {
		glob_config->port = 11215;
		g_clear_error(&err);
	}

	glob_config->maxidle = g_key_file_get_integer(keyfile, "General",
			"maxidle", &err);
	if (err) {
		glob_config->maxidle = 20;
		g_clear_error(&err);
	}
	glob_config->maxconns = g_key_file_get_integer(keyfile, "General",
			"maxconns", &err);
	if (err) {
		glob_config->maxconns = 4096;
		g_clear_error(&err);
	}
	glob_config->host_ip = g_key_file_get_string(keyfile, "General", "host_ip",
			&err);
	if (err) {
		glob_config->host_ip = NULL;
		g_clear_error(&err);
	}
	int index;
	char **node;

	gsize node_length;
	node = g_key_file_get_string_list(keyfile, "General", "masters",
			&node_length, &err);
	if (err) {
		fprintf(stderr, "at least one master memcached server \n");
		exit(-1);
	}

	for (index = 0; index < node_length; index++) {
		add_master_memcached(node[index]);
	}
	g_strfreev(node);

	node = g_key_file_get_string_list(keyfile, "General", "backups",
			&node_length, &err);
	if (err) {
		//fprintf(stderr,"at least one master memcached server \n");
		//g_strfreev(node);
		return;
	}

	for (index = 0; index < node_length; index++) {
		add_backup_memcached(node[index]);
	}
	g_strfreev(node);

}

void init_ketama(){

	int i;
	char temp[64];
    ketama = (struct ketama*)calloc(sizeof (struct ketama), 1);
    if (ketama == NULL) {
			fprintf(stderr, "not enough memory to create ketama\n");
			exit(1);
		} else {
			ketama->count = matrixcnt;
			ketama->weight = (int *) calloc(sizeof(int), ketama->count);
			ketama->name = (char **) calloc(sizeof(char *), ketama->count);

			if (ketama->weight == NULL || ketama->name == NULL) {
				fprintf(stderr, "not enough memory to create ketama\n");
				exit(1);
			}

			for (i = 0; i < ketama->count; i++) {
				ketama->weight[i] = 100;
				ketama->totalweight += ketama->weight[i];
				snprintf(temp, sizeof(temp), "%s-%d", matrixs[i].ip,
						matrixs[i].port);
				ketama->name[i] = strdup(temp);
				if (ketama->name[i] == NULL) {
					fprintf(stderr, "not enough memory to create ketama\n");
					exit(1);
				}
			}
		}
    if (create_ketama(ketama, 500)) {
			fprintf(stderr, "can't create ketama\n");
			exit(1);
		}
    /* update backup server ketama */
    if (backupcnt > 0) {
			backupkt = (struct ketama *) calloc(sizeof(struct ketama), 1);
			if (backupkt == NULL) {
				fprintf(stderr, "not enough memory to create ketama\n");
				exit(1);
			} else {
				backupkt->count = backupcnt;
				backupkt->weight = (int *) calloc(sizeof(int), backupkt->count);
				backupkt->name = (char **) calloc(sizeof(char *),
						backupkt->count);

				if (backupkt->weight == NULL || backupkt->name == NULL) {
					fprintf(stderr, "not enough memory to create ketama\n");
					exit(1);
				}

				for (i = 0; i < backupkt->count; i++) {
					backupkt->weight[i] = 100;
					backupkt->totalweight += backupkt->weight[i];
					snprintf(temp, sizeof(temp), "%s-%d", backups[i].ip,
							backups[i].port);
					backupkt->name[i] = strdup(temp);
					if (backupkt->name[i] == NULL) {
						fprintf(stderr, "not enough memory to create ketama\n");
						exit(1);
					}
				}
			}
			if (create_ketama(backupkt, 500)) {
				fprintf(stderr, "can't create backup ketama\n");
				exit(1);
			}
		}
  //  fprintf(stderr, "using ketama algorithm\n");
}

static void init_global_config(void) {


	log_init(glob_config->logdir, L_INFO, LOG_PROGRAMME);

	if (glob_config->port == 0) {
		jlog(L_ERR, "magent must listen on tcp \n");
		exit(1);
	}

	if (glob_config->daemon && daemon(0, 0) == -1) {
		jlog(L_ERR, "failed to be a daemon\n");
		exit(1);
	}
	if (glob_config->useketama) {
    init_ketama();
	}
}

static void init_socket() {
	struct sockaddr_in server;

	int flags = 1;

	struct linger ling = { 0, 0 };

	if (glob_config->port) {
		sockfd = socket(AF_INET, SOCK_STREAM, 0);
		if (sockfd < 0) {
			jlog(L_ERR, "CAN'T CREATE NETWORK SOCKET\n");
			exit(1);
		}

		fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFL) | O_NONBLOCK);

		setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (void *) &flags,
				sizeof(flags));
		setsockopt(sockfd, SOL_SOCKET, SO_LINGER, (void *) &ling, sizeof(ling));
		setsockopt(sockfd, SOL_SOCKET, SO_KEEPALIVE, (void *) &flags,
				sizeof(flags));

		memset((char *) &server, 0, sizeof(server));
		server.sin_family = AF_INET;
		if (glob_config->host_ip == NULL)
			server.sin_addr.s_addr = htonl(INADDR_ANY);
		else
			server.sin_addr.s_addr = inet_addr(glob_config->host_ip);

		server.sin_port = htons(glob_config->port);

		if (bind(sockfd, (struct sockaddr *) &server, sizeof(server))) {
			if (errno != EINTR)
				jlog(L_ERR, "bind errno = %d: %s\n", errno, strerror(errno));
			close(sockfd);
			exit(1);
		}

		if (listen(sockfd, 1024)) {
			jlog(L_ERR, "listen errno = %d: %s\n", errno, strerror(errno));
			close(sockfd);
			exit(1);
		}

	}

}

void init_config(const char *config_file) {
	GKeyFile *keyfile;
	keyfile = load_config(config_file);
	glob_config = calloc(sizeof(s_config), 1);
	if (NULL == glob_config) {
		fprintf(stderr, "calloc glob_config failed\n");
		exit(-1);
	}
	init_general_config(keyfile);
	init_global_config();
	g_key_file_free(keyfile);

}

void config_free() {
	free(glob_config->logdir);
	free(glob_config);
}

static void server_error(conn *c, const char *s) {
	int i;

	if (c == NULL)
		return;

	if (c->srv) {
		server_free(c->srv);
		c->srv = NULL;
	}

	if (c->keys) {
		for (i = 0; i < c->keycount; i++)
			free(c->keys[i]);
		free(c->keys);
		c->keys = NULL;
	}

	c->pos = c->keycount = c->keyidx = 0;
	list_free(c->request, 1);
	list_free(c->response, 1);
	out_string(c, s);
	c->state = CLIENT_COMMAND;
}

/* return 0 if ok, return 1 if failed */
static int socket_connect(struct server *s) {
	socklen_t servlen;

	if (s == NULL || s->sfd <= 0 || s->state != SERVER_INIT)
		return 1;

	servlen = sizeof(s->owner->dstaddr);
	if (-1
			== connect(s->sfd, (struct sockaddr *) &(s->owner->dstaddr),
					servlen)) {
		if (errno != EINPROGRESS && errno != EALREADY)
			return 1;
		s->state = SERVER_CONNECTING;
	} else {
		s->state = SERVER_CONNECTED;
	}

	return 0;
}

static void conn_close(conn *c) {
	int i;

	if (c == NULL)
		return;

	/* check client connection */
	if (c->cfd > 0) {
		if (glob_config->verbose_mode)
			fprintf(stderr, "%s: (%s.%d) CLOSE CLIENT CONNECTION FD %d\n",
					cur_ts_str, __FILE__, __LINE__, c->cfd);
		event_del(&(c->ev));
		close(c->cfd);
		curconns--;
		c->cfd = 0;
	}

	server_free(c->srv);

	if (c->keys) {
		for (i = 0; i < c->keycount; i++)
			free(c->keys[i]);
		free(c->keys);
		c->keys = NULL;
	}

	list_free(c->request, 0);
	list_free(c->response, 0);
	free(c);
}

/* ------------- from lighttpd's network_writev.c ------------ */

#ifndef UIO_MAXIOV
# if defined(__FreeBSD__) || defined(__APPLE__) || defined(__NetBSD__)
/* FreeBSD 4.7 defines it in sys/uio.h only if _KERNEL is specified */
#  define UIO_MAXIOV 1024
# elif defined(__sgi)
/* IRIX 6.5 has sysconf(_SC_IOV_MAX) which might return 512 or bigger */
#  define UIO_MAXIOV 512
# elif defined(__sun)
/* Solaris (and SunOS?) defines IOV_MAX instead */
#  ifndef IOV_MAX
#   define UIO_MAXIOV 16
#  else
#   define UIO_MAXIOV IOV_MAX
#  endif
# elif defined(IOV_MAX)
#  define UIO_MAXIOV IOV_MAX
# else
#  error UIO_MAXIOV nor IOV_MAX are defined
# endif
#endif

/* return 0 if success */
static int writev_list(int fd, list *l) {
	size_t num_chunks, i, num_bytes = 0, toSend, r, r2;
	struct iovec chunks[UIO_MAXIOV];
	buffer *b;

	if (l == NULL || l->first == NULL || fd <= 0)
		return 0;

	for (num_chunks = 0, b = l->first; b && num_chunks < UIO_MAXIOV; num_chunks++, b
			= b->next)
		;

	for (i = 0, b = l->first; i < num_chunks; b = b->next, i++) {
		if (b->size == 0) {
			num_chunks--;
			i--;
		} else {
			chunks[i].iov_base = b->ptr + b->used;
			toSend = b->size - b->used;

			/* protect the return value of writev() */
			if (toSend > SSIZE_MAX || (num_bytes + toSend) > SSIZE_MAX) {
				chunks[i].iov_len = SSIZE_MAX - num_bytes;

				num_chunks = i + 1;
				break;
			} else {
				chunks[i].iov_len = toSend;
			}

			num_bytes += toSend;
		}
	}

	if ((r = writev(fd, chunks, num_chunks)) < 0) {
		switch (errno) {
		case EAGAIN:
		case EINTR:
			return 0; /* try again */
			break;
		case EPIPE:
		case ECONNRESET:
			return -2; /* connection closed */
			break;
		default:
			return -1; /* error */
			break;
		}
	}

	r2 = r;

	for (i = 0, b = l->first; i < num_chunks; b = b->next, i++) {
		if (r >= (ssize_t) chunks[i].iov_len) {
			r -= chunks[i].iov_len;
			b->used += chunks[i].iov_len;
		} else {
			/* partially written */
			b->used += r;
			break;
		}
	}

	remove_finished_buffers(l);
	return r2;
}

/* --------- end here ----------- */

static void out_string(conn *c, const char *str) {
	/* append str to c->wbuf */
	int len = 0;
	buffer *b;

	if (c == NULL || str == NULL || str[0] == '\0')
		return;

	len = strlen(str);

	b = buffer_init_size(len + 3);
	if (b == NULL)
		return;

	memcpy(b->ptr, str, len);
	memcpy(b->ptr + len, "\r\n", 2);
	b->size = len + 2;
	b->ptr[b->size] = '\0';

	append_buffer_to_list(c->response, b);

	if (writev_list(c->cfd, c->response) >= 0) {
		if (c->response->first && (c->ev_flags != EV_WRITE)) {
			/* update event handler */
			event_del(&(c->ev));
			event_set(&(c->ev), c->cfd, EV_WRITE | EV_PERSIST, drive_client,
					(void *) c);
			event_add(&(c->ev), 0);
			c->ev_flags = EV_WRITE;
		}
	} else {
		/* client reset/close connection*/
		conn_close(c);
	}
}

/* finish proxy transcation */
static void finish_transcation(conn *c) {
	int i;

	if (c == NULL)
		return;

	if (c->keys) {
		for (i = 0; i < c->keycount; i++)
			free(c->keys[i]);
		free(c->keys);
		c->keys = NULL;
		c->keycount = c->keyidx = 0;
	}

	c->state = CLIENT_COMMAND;
	list_free(c->request, 1);
}

static void start_update_backupserver(conn *c) {
	int size = 0, idx;
	buffer *b, *r;
	matrix *m;
	server *s;

	if (c == NULL)
		return;

	if (c->flag.is_update_cmd == 0 || backupcnt == 0 || c->keycount != 1)
		return;

	/* start backup set server now */
	b = c->request->first;
	while (b) {
		size += b->size;
		b = b->next;
	}

	if (size == 0)
		return;

	r = buffer_init_size(size + 1);
	if (r == NULL)
		return;

	b = c->request->first;
	while (b) {
		if (b->size > 0) {
			memcpy(r->ptr + r->size, b->ptr, b->size);
			r->size += b->size;
		}
		b = b->next;
	}

	if (glob_config->useketama && backupkt) {
		idx = get_server(backupkt, c->keys[0]);
		if (idx < 0) {
			/* fall back to round selection */
			idx = hashme(c->keys[0]) % backupcnt;
		}
	} else {
		/* just round selection */
		idx = hashme(c->keys[0]) % backupcnt;
	}

	m = backups + idx;

	if (m->pool && (m->used > 0)) {
		s = m->pool[--m->used];
		s->pool_idx = 0;
		if (glob_config->verbose_mode)
			fprintf(stderr, "%s: (%s.%d) GET SERVER FD %d <- POOL\n",
					cur_ts_str, __FILE__, __LINE__, s->sfd);
	} else {
		s = (struct server *) calloc(sizeof(struct server), 1);
		if (s == NULL) {
			buffer_free(r);
			return;
		}
		s->request = list_init();
		s->response = list_init();
		s->state = SERVER_INIT;
	}
	s->owner = m;

	if (glob_config->verbose_mode)
		fprintf(stderr, "%s: (%s.%d) BACKUP KEY \"%s\" -> %s:%d\n", cur_ts_str,
				__FILE__, __LINE__, c->keys[0], m->ip, m->port);

	if (s->sfd <= 0) {
		s->sfd = socket(AF_INET, SOCK_STREAM, 0);
		if (s->sfd < 0) {
			fprintf(stderr,
					"%s: (%s.%d) CAN'T CREATE TCP SOCKET TO MEMCACHED\n",
					cur_ts_str, __FILE__, __LINE__);
			free(s);
			buffer_free(r);
			return;
		}
		fcntl(s->sfd, F_SETFL, fcntl(s->sfd, F_GETFL) | O_NONBLOCK);
	}

	append_buffer_to_list(s->request, r);

	if (s->state == SERVER_INIT && socket_connect(s)) {
		/* server error */
		server_free(s);
		return;
	}

	/* server event handler */
	memset(&(s->ev), 0, sizeof(struct event));
	event_set(&(s->ev), s->sfd, EV_PERSIST | EV_WRITE, drive_backup_server,
			(void *) s);
	event_add(&(s->ev), 0);
}

/* start whole memcache agent transcation */
static void start_magent_transcation(conn *c) {
	if (c == NULL)
		return;

	if (c->flag.is_update_cmd && backupcnt > 0 && c->keycount == 1)
		start_update_backupserver(c);

	/* start first transaction to normal server */
	do_transcation(c);
}

/* start/repeat memcached proxy transcations */
static void do_transcation(conn *c) {
	int idx;
	struct matrix *m;
	struct server *s;
	char *key = NULL;
	buffer *b;

	if (c == NULL)
		return;

	c->flag.is_backup = 0;

	if (c->flag.is_get_cmd) {
		if (c->keyidx >= c->keycount) {
			/* end of get transcation */
			finish_transcation(c);
			return;
		}
		key = c->keys[c->keyidx++];
		if (c->keyidx == c->keycount)
			c->flag.is_last_key = 1;
	} else {
		key = c->keys[0];
	}

	if (glob_config->useketama && ketama) {
		idx = get_server(ketama, key);
		if (idx < 0) {
			/* fall back to round selection */
			idx = hashme(key) % matrixcnt;
		}
	} else {
		/* just round selection */
		idx = hashme(key) % matrixcnt;
	}

	m = matrixs + idx;

	if (m->pool && (m->used > 0)) {
		s = m->pool[--m->used];
		s->pool_idx = 0;
		if (glob_config->verbose_mode)
			fprintf(stderr, " (%s.%d) GET SERVER FD %d <- POOL\n", __FILE__,
					__LINE__, s->sfd);
	} else {
		s = (struct server *) calloc(sizeof(struct server), 1);
		if (s == NULL) {
			fprintf(stderr, " (%s.%d) SERVER OUT OF MEMORY\n", __FILE__,
					__LINE__);
			conn_close(c);
			return;
		}
		s->request = list_init();
		s->response = list_init();
		s->state = SERVER_INIT;
	}
	s->owner = m;
	c->srv = s;

	if (glob_config->verbose_mode)
		fprintf(stderr, "%s: (%s.%d) %s KEY \"%s\" -> %s:%d\n", cur_ts_str,
				__FILE__, __LINE__, c->flag.is_get_cmd ? "GET" : "SET", key,
				m->ip, m->port);

	if (s->sfd <= 0) {
		s->sfd = socket(AF_INET, SOCK_STREAM, 0);
		if (s->sfd < 0) {
			jlog(L_ERR, "(%s.%d) CAN'T CREATE TCP SOCKET TO MEMCACHED\n",
					__FILE__, __LINE__);
			server_error(c, "SERVER_ERROR CAN NOT CONNECT TO BACKEND");
			return;
		}
		fcntl(s->sfd, F_SETFL, fcntl(c->srv->sfd, F_GETFL) | O_NONBLOCK);
		memset(&(s->ev), 0, sizeof(struct event));
	} else {
		event_del(&(c->srv->ev)); /* delete previous pool handler */
		s->state = SERVER_CONNECTED;
	}

	/* reset flags */
	s->has_response_header = 0;
	s->remove_trail = 0;
	s->valuebytes = 0;

	if (c->flag.is_get_cmd) {
		b = buffer_init_size(strlen(key) + 20);
		if (b == NULL) {
			fprintf(stderr, "%s: (%s.%d) SERVER OUT OF MEMORY\n", cur_ts_str,
					__FILE__, __LINE__);
			server_error(c, "SERVER_ERROR OUT OF MEMORY");
			return;
		}
		b->size = snprintf(b->ptr, b->len - 1, "%s %s\r\n",
				c->flag.is_gets_cmd ? "gets" : "get", key);
		append_buffer_to_list(s->request, b);
	} else {
		copy_list(c->request, s->request);
	}

	c->state = CLIENT_TRANSCATION;
	/* server event handler */

	if (s->state == SERVER_INIT && socket_connect(s)) {
		if (backupcnt > 0)
			try_backup_server(c);
		else
			server_error(c, "SERVER_ERROR CAN NOT CONNECT TO BACKEND SERVER");
		return;
	}

	event_set(&(s->ev), s->sfd, EV_PERSIST | EV_WRITE, drive_memcached_server,
			(void *) c);
	event_add(&(s->ev), 0);
	s->ev_flags = EV_WRITE;
}

static void try_backup_server(conn *c) {
	int idx;
	struct matrix *m;
	char *key;
	buffer *b;
	struct server *s;

	if (c == NULL)
		return;

	/* free previous error server */
	server_free(c->srv);
	c->srv = NULL;

	if (c->flag.is_backup || c->flag.is_incr_decr_cmd || backups == NULL) {
		/* don't duplicate incr/decr cmds */
		/* already tried backup server or no backup server*/
		do_transcation(c);
		return;
	}

	c->flag.is_backup = 1;

	if (c->flag.is_get_cmd)
		key = c->keys[c->keyidx - 1];
	else
		key = c->keys[0];

	if (glob_config->useketama && backupkt) {
		idx = get_server(backupkt, key);
		if (idx < 0) {
			/* fall back to round selection */
			idx = hashme(key) % backupcnt;
		}
	} else {
		/* just round selection */
		idx = hashme(key) % backupcnt;
	}

	m = backups + idx;

	if (glob_config->verbose_mode)
		fprintf(stderr, "%s: (%s.%d) TRYING BACKUP SERVER %s:%d\n", cur_ts_str,
				__FILE__, __LINE__, m->ip, m->port);

	if (m->pool && (m->used > 0)) {
		s = m->pool[--m->used];
		s->pool_idx = 0;
		if (glob_config->verbose_mode)
			fprintf(stderr, "%s: (%s.%d) GET SERVER FD %d <- POOL\n",
					cur_ts_str, __FILE__, __LINE__, s->sfd);
	} else {
		s = (struct server *) calloc(sizeof(struct server), 1);
		if (s == NULL) {
			fprintf(stderr, "%s: (%s.%d) SERVER OUT OF MEMORY\n", cur_ts_str,
					__FILE__, __LINE__);
			conn_close(c);
			return;
		}
		s->request = list_init();
		s->response = list_init();
		memset(&(s->ev), 0, sizeof(struct event));
		s->state = SERVER_INIT;
	}
	s->owner = m;
	c->srv = s;

	if (glob_config->verbose_mode)
		fprintf(stderr, "%s: (%s.%d) %s KEY \"%s\" -> %s:%d\n", cur_ts_str,
				__FILE__, __LINE__, c->flag.is_get_cmd ? "GET" : "SET", key,
				m->ip, m->port);

	if (s->sfd <= 0) {
		s->sfd = socket(AF_INET, SOCK_STREAM, 0);
		if (s->sfd < 0) {
			fprintf(stderr,
					"%s: (%s.%d) CAN'T CREATE TCP SOCKET TO MEMCACHED\n",
					cur_ts_str, __FILE__, __LINE__);
			server_error(c, "SERVER_ERROR CAN NOT CONNECT TO BACKEND");
			return;
		}
		fcntl(s->sfd, F_SETFL, fcntl(s->sfd, F_GETFL) | O_NONBLOCK);
	} else {
		event_del(&(s->ev)); /* delete previous pool handle handler */
		s->state = SERVER_CONNECTED;
	}

	/* reset flags */
	s->has_response_header = 0;
	s->remove_trail = 0;
	s->valuebytes = 0;

	if (c->flag.is_get_cmd) {
		b = buffer_init_size(strlen(key) + 20);
		if (b == NULL) {
			fprintf(stderr, "%s: (%s.%d) SERVER OUT OF MEMORY\n", cur_ts_str,
					__FILE__, __LINE__);
			server_error(c, "SERVER_ERROR OUT OF MEMORY");
			return;
		}
		b->size = snprintf(b->ptr, b->len - 1, "%s %s\r\n",
				c->flag.is_gets_cmd ? "gets" : "get", key);
		append_buffer_to_list(s->request, b);
	} else {
		copy_list(c->request, s->request);
	}

	c->state = CLIENT_TRANSCATION;
	/* server event handler */

	if (s->state == SERVER_INIT && socket_connect(s)) {
		server_free(s);
		server_error(c, "SERVER_ERROR CAN NOT CONNECT TO BACKEND SERVER");
		return;
	}

	event_set(&(s->ev), s->sfd, EV_PERSIST | EV_WRITE, drive_memcached_server,
			(void *) c);
	event_add(&(s->ev), 0);
	s->ev_flags = EV_WRITE;
}

static void drive_memcached_server(const int fd, const short which, void *arg) {
	struct server *s;
	conn *c;
	int socket_error, r, toread;
	socklen_t servlen, socket_error_len;

	if (arg == NULL)
		return;
	c = (conn *) arg;

	s = c->srv;
	if (s == NULL)
		return;

	if (which & EV_WRITE) {
		switch (s->state) {
		case SERVER_INIT:
			servlen = sizeof(s->owner->dstaddr);
			if (-1 == connect(s->sfd, (struct sockaddr *) &(s->owner->dstaddr),
					servlen)) {
				if (errno != EINPROGRESS && errno != EALREADY) {
					if (glob_config->verbose_mode)
						fprintf(
								stderr,
								"%s: (%s.%d) CAN'T CONNECT TO MAIN SERVER %s:%d\n",
								cur_ts_str, __FILE__, __LINE__, s->owner->ip,
								s->owner->port);

					if (backupcnt > 0)
						try_backup_server(c);
					else
						server_error(c,
								"SERVER_ERROR CAN NOT CONNECT TO BACKEND SERVER");
					return;
				}
			}
			s->state = SERVER_CONNECTING;
			break;

		case SERVER_CONNECTING:
			socket_error_len = sizeof(socket_error);
			/* try to finish the connect() */
			if ((0 != getsockopt(s->sfd, SOL_SOCKET, SO_ERROR, &socket_error,
					&socket_error_len)) || (socket_error != 0)) {
				if (glob_config->verbose_mode)
					fprintf(stderr,
							"%s: (%s.%d) CAN'T CONNECT TO MAIN SERVER %s:%d\n",
							cur_ts_str, __FILE__, __LINE__, s->owner->ip,
							s->owner->port);

				if (backupcnt > 0)
					try_backup_server(c);
				else
					server_error(c,
							"SERVER_ERROR CAN NOT CONNECT TO BACKEND SERVER");
				return;
			}

			if (glob_config->verbose_mode)
				fprintf(stderr, "%s: (%s.%d) CONNECTED FD %d <-> %s:%d\n",
						cur_ts_str, __FILE__, __LINE__, s->sfd, s->owner->ip,
						s->owner->port);

			s->state = SERVER_CONNECTED;
			break;

		case SERVER_CONNECTED:
			/* write request to memcached server */
			r = writev_list(s->sfd, s->request);
			if (r < 0) {
				/* write failed */
				server_error(c,
						"SERVER_ERROR CAN NOT WRITE REQUEST TO BACKEND SERVER");
				return;
			} else {
				if (s->request->first == NULL) {
					/* finish writing request to memcached server */
					if (c->flag.no_reply) {
						finish_transcation(c);
					} else if (s->ev_flags != EV_READ) {
						event_del(&(s->ev));
						event_set(&(s->ev), s->sfd, EV_PERSIST | EV_READ,
								drive_memcached_server, arg);
						event_add(&(s->ev), 0);
						s->ev_flags = EV_READ;
					}
				}
			}
			break;

		case SERVER_ERROR:
			server_error(c, "SERVER_ERROR BACKEND SERVER ERROR");
			break;
		}
		return;
	}

	if (!(which & EV_READ))
		return;

	/* get the byte counts of read */
	if (ioctl(s->sfd, FIONREAD, &toread) || toread == 0) {
		if (backupcnt > 0)
			try_backup_server(c);
		else
			server_error(c,
					"SERVER_ERROR BACKEND SERVER RESET OR CLOSE CONNECTION");
		return;
	}

	if (c->flag.is_get_cmd) {
		if (s->has_response_header == 0) {
			/* NO RESPONSE HEADER */
			if (toread > (BUFFERLEN - s->pos))
				toread = BUFFERLEN - s->pos;
		} else {
			/* HAS RESPONSE HEADER */
			if (toread > (BUFFERLEN - s->pos))
				toread = BUFFERLEN - s->pos;
			if (toread > s->valuebytes)
				toread = s->valuebytes;
		}
	} else {
		if (toread > (BUFFERLEN - s->pos))
			toread = BUFFERLEN - s->pos;
	}

	r = read(s->sfd, s->line + s->pos, toread);
	if (r <= 0) {
		if (r == 0 || (errno != EAGAIN && errno != EINTR)) {
			if (backupcnt > 0)
				try_backup_server(c);
			else
				server_error(c, "SERVER_ERROR BACKEND SERVER CLOSE CONNECTION");
		}
		return;
	}

	s->pos += r;
	s->line[s->pos] = '\0';

	if (c->flag.is_get_cmd)
		process_get_response(c, r);
	else
		process_update_response(c);
}

static void process_get_response(conn *c, int r) {
	struct server *s;
	buffer *b;
	int pos;

	if (c == NULL || c->srv == NULL || c->srv->pos == 0)
		return;
	s = c->srv;
	if (s->has_response_header == 0) {
		pos = memstr(s->line, "\n", s->pos, 1);

		if (pos == -1)
			return; /* not found */

		/* found \n */
		s->has_response_header = 1;
		s->remove_trail = 0;
		pos++;

		s->valuebytes = -1;

		/* VALUE <key> <flags> <bytes> [<cas unique>]\r\n
		 * END\r\n*/
		if (strncasecmp(s->line, "VALUE ", 6) == 0) {
			char *p = NULL;
			p = strchr(s->line + 6, ' ');
			if (p) {
				p = strchr(p + 1, ' ');
				if (p) {
					s->valuebytes = atol(p + 1);
					if (s->valuebytes < 0) {
						try_backup_server(c); /* conn_close(c); */
					}
				}
			}
		}

		if (s->valuebytes < 0) {
			/* END\r\n or SERVER_ERROR\r\n
			 * just skip this transcation
			 */
			put_server_into_pool(s);
			c->srv = NULL;
			if (c->flag.is_last_key)
				out_string(c, "END");
			do_transcation(c); /* TO Next KEY */
			return;
		}
		s->valuebytes += 7; /* trailing \r\nEND\r\n */

		b = buffer_init_size(pos + 1);
		if (b == NULL) {
			fprintf(stderr, "%s: (%s.%d) SERVER OUT OF MEMORY\n", cur_ts_str,
					__FILE__, __LINE__);
			try_backup_server(c); /* conn_close(c); */
			return;
		}
		memcpy(b->ptr, s->line, pos);
		b->size = pos;
		append_buffer_to_list(s->response, b);

		if (s->pos > pos) {
			memmove(s->line, s->line + pos, s->pos - pos);
			s->pos -= pos;
		} else {
			s->pos = 0;
		}

		if (s->pos > 0)
			s->valuebytes -= s->pos;
	} else {
		/* HAS RESPONSE HEADER */
		s->valuebytes -= r;
	}

	if (s->remove_trail) {
		s->pos = 0;
	} else if (s->pos > 0) {
		b = buffer_init_size(s->pos + 1);
		if (b == NULL) {
			fprintf(stderr, "%s: (%s.%d) SERVER OUT OF MEMORY\n", cur_ts_str,
					__FILE__, __LINE__);
			try_backup_server(c); /* conn_close(c); */
			return;
		}
		memcpy(b->ptr, s->line, s->pos);
		b->size = s->pos;

		if (s->valuebytes <= 5) {
			b->size -= (5 - s->valuebytes); /* remove trailing END\r\n */
			s->remove_trail = 1;
		}
		s->pos = 0;

		append_buffer_to_list(s->response, b);
	}

	if (s->valuebytes == 0) {
		/* GET commands finished, go on next memcached server */
		move_list(s->response, c->response);
		put_server_into_pool(s);
		c->srv = NULL;
		if (c->flag.is_last_key) {
			b = buffer_init_size(6);
			if (b) {
				memcpy(b->ptr, "END\r\n", 5);
				b->size = 5;
				b->ptr[b->size] = '\0';
				append_buffer_to_list(c->response, b);
			} else {
				fprintf(stderr, "%s: (%s.%d) OUT OF MEMORY\n", cur_ts_str,
						__FILE__, __LINE__);
			}
		}

		if (writev_list(c->cfd, c->response) >= 0) {
			if (c->response->first && (c->ev_flags != EV_WRITE)) {
				event_del(&(c->ev));
				event_set(&(c->ev), c->cfd, EV_WRITE | EV_PERSIST,
						drive_client, (void *) c);
				event_add(&(c->ev), 0);
				c->ev_flags = EV_WRITE;
			}
			do_transcation(c); /* NEXT MEMCACHED SERVER */
		} else {
			/* client reset/close connection*/
			conn_close(c);
		}
	}

}

static void process_update_response(conn *c) {
	struct server *s;
	buffer *b;
	int pos;

	if (c == NULL || c->srv == NULL || c->srv->pos == 0)
		return;
	s = c->srv;
#if 0
	pos = memstr(s->line, "\n", s->pos, 1);
	if (pos == -1) return; /* not found */
#else
	if (s->line[s->pos - 1] != '\n')
		return;
	pos = s->pos - 1;
#endif
	/* found \n */
	pos++;

	b = buffer_init_size(pos + 1);
	if (b == NULL) {
		fprintf(stderr, "%s: (%s.%d) SERVER OUT OF MEMORY\n", cur_ts_str,
				__FILE__, __LINE__);
		server_error(c, "SERVER_ERROR OUT OF MEMORY");
		return;
	}
	memcpy(b->ptr, s->line, pos);
	b->size = pos;

	append_buffer_to_list(s->response, b);
	move_list(s->response, c->response);
	put_server_into_pool(s);
	c->srv = NULL;
	if (writev_list(c->cfd, c->response) >= 0) {
		if (c->response->first && (c->ev_flags != EV_WRITE)) {
			event_del(&(c->ev));
			event_set(&(c->ev), c->cfd, EV_WRITE | EV_PERSIST, drive_client,
					(void *) c);
			event_add(&(c->ev), 0);
			c->ev_flags = EV_WRITE;
		}
		finish_transcation(c);
	} else {
		/* client reset/close connection*/
		conn_close(c);
	}
}

static void drive_backup_server(const int fd, const short which, void *arg) {
	struct server *s;
	int socket_error, r, toread, pos;
	socklen_t servlen, socket_error_len;

	if (arg == NULL)
		return;
	s = (struct server *) arg;

	if (which & EV_WRITE) {
		switch (s->state) {
		case SERVER_INIT:
			servlen = sizeof(s->owner->dstaddr);
			if (-1 == connect(s->sfd, (struct sockaddr *) &(s->owner->dstaddr),
					servlen)) {
				if (errno != EINPROGRESS && errno != EALREADY) {
					if (glob_config->verbose_mode)
						fprintf(
								stderr,
								"%s: (%s.%d) CAN'T CONNECT TO BACKUP SERVER %s:%d\n",
								cur_ts_str, __FILE__, __LINE__, s->owner->ip,
								s->owner->port);

					server_free(s);
					return;
				}
			}
			s->state = SERVER_CONNECTING;
			break;

		case SERVER_CONNECTING:
			socket_error_len = sizeof(socket_error);
			/* try to finish the connect() */
			if ((0 != getsockopt(s->sfd, SOL_SOCKET, SO_ERROR, &socket_error,
					&socket_error_len)) || (socket_error != 0)) {
				if (glob_config->verbose_mode)
					fprintf(
							stderr,
							"%s: (%s.%d) CAN'T CONNECT TO BACKUP SERVER %s:%d\n",
							cur_ts_str, __FILE__, __LINE__, s->owner->ip,
							s->owner->port);

				server_free(s);
				return;
			}

			if (glob_config->verbose_mode)
				fprintf(stderr,
						"%s: (%s.%d) CONNECTED BACKUP FD %d <-> %s:%d\n",
						cur_ts_str, __FILE__, __LINE__, s->sfd, s->owner->ip,
						s->owner->port);

			s->state = SERVER_CONNECTED;
			break;

		case SERVER_CONNECTED:
			/* write request to memcached server */
			r = writev_list(s->sfd, s->request);
			if (r < 0) {
				/* write failed */
				server_free(s);
				return;
			} else {
				if (s->request->first == NULL) {
					event_del(&(s->ev));
					event_set(&(s->ev), s->sfd, EV_PERSIST | EV_READ,
							drive_backup_server, arg);
					event_add(&(s->ev), 0);
				}
			}
			break;

		case SERVER_ERROR:
			server_free(s);
			break;
		}
		return;
	}

	if (!(which & EV_READ))
		return;

	/* get the byte counts of read */
	if (ioctl(s->sfd, FIONREAD, &toread) || toread == 0) {
		/* ioctl error or memcached server close/reset connection */
		server_free(s);
		return;
	}

	if (toread > (BUFFERLEN - s->pos))
		toread = BUFFERLEN - s->pos;

	r = read(s->sfd, s->line + s->pos, toread);
	if (r <= 0) {
		if (r == 0 || (errno != EAGAIN && errno != EINTR))
			server_free(s);
		return;
	}

	s->pos += r;
	s->line[s->pos] = '\0';

	pos = memstr(s->line, "\n", s->pos, 1);

	if (pos == -1)
		return; /* not found */

	/* put backup connection into pool */
	put_server_into_pool(s);
}

/* return 1 if command found
 * return 0 if not found
 */
static void process_command(conn *c) {
	char *p;
	int len, skip = 0, i, j;
	buffer *b;
	token_t tokens[MAX_TOKENS];
	size_t ntokens;

	if (c->state != CLIENT_COMMAND)
		return;

	p = strchr(c->line, '\n');
	if (p == NULL)
		return;

	len = p - c->line;
	*p = '\0'; /* remove \n */
	if (*(p - 1) == '\r') {
		*(p - 1) = '\0'; /* remove \r */
		len--;
	}

	/* backup command line buffer first */
	b = buffer_init_size(len + 3);
	memcpy(b->ptr, c->line, len);
	b->ptr[len] = '\r';
	b->ptr[len + 1] = '\n';
	b->ptr[len + 2] = '\0';
	b->size = len + 2;

#if 0
	if (verbose_mode)
	fprintf(stderr, "%s: (%s.%d) PROCESSING COMMAND: %s", cur_ts_str, __FILE__, __LINE__, b->ptr);
#endif

	memset(&(c->flag), 0, sizeof(c->flag));
	c->flag.is_update_cmd = 1;
	c->storebytes = c->keyidx = 0;

	ntokens = tokenize_command(c->line, tokens, MAX_TOKENS);
	if (ntokens >= 3 && ((strcmp(tokens[COMMAND_TOKEN].value, "get") == 0)
			|| (strcmp(tokens[COMMAND_TOKEN].value, "gets") == 0))) {
		/*
		 * get/gets <key>*\r\n
		 *
		 * VALUE <key> <flags> <bytes> [<cas unique>]\r\n
		 * <data block>\r\n
		 * "END\r\n"
		 */
		c->keycount = ntokens - KEY_TOKEN - 1;
		c->keys = (char **) calloc(sizeof(char *), c->keycount);
		if (c->keys == NULL) {
			c->keycount = 0;
			out_string(c, "SERVER_ERROR OUT OF MEMORY");
			skip = 1;
		} else {
			if (ntokens < MAX_TOKENS) {
				for (i = KEY_TOKEN, j = 0; (i < ntokens) && (j < c->keycount); i++, j++)
					c->keys[j] = strdup(tokens[i].value);
			} else {
				char *pp, **nn;

				for (i = KEY_TOKEN, j = 0; (i < (MAX_TOKENS - 1)) && (j
						< c->keycount); i++, j++)
					c->keys[j] = strdup(tokens[i].value);

				if (tokens[MAX_TOKENS - 1].value != NULL) {
					/* check for last TOKEN */
					pp = strtok(tokens[MAX_TOKENS - 1].value, " ");

					while (pp != NULL) {
						nn = (char **) realloc(c->keys, (c->keycount + 1)
								* sizeof(char *));
						if (nn == NULL) {
							/* out of memory */
							break;
						}
						c->keys = nn;
						c->keys[c->keycount] = strdup(pp);
						c->keycount++;
						pp = strtok(NULL, " ");
					}
				} else {
					/* last key is NULL, set keycount to actual number*/
					c->keycount = j;
				}
			}

			c->flag.is_get_cmd = 1;
			c->keyidx = 0;
			c->flag.is_update_cmd = 0;

			if (strcmp(tokens[COMMAND_TOKEN].value, "gets") == 0)
				c->flag.is_gets_cmd = 1; /* GETS */
		}
	} else if ((ntokens == 4 || ntokens == 5) && ((strcmp(
			tokens[COMMAND_TOKEN].value, "decr") == 0) || (strcmp(
			tokens[COMMAND_TOKEN].value, "incr") == 0))) {
		/*
		 * incr <key> <value> [noreply]\r\n
		 * decr <key> <value> [noreply]\r\n
		 *
		 * "NOT_FOUND\r\n" to indicate the item with this value was not found
		 * <value>\r\n , where <value> is the new value of the item's data,
		 */
		c->flag.is_incr_decr_cmd = 1;
	} else if (ntokens >= 3 && ntokens <= 5 && (strcmp(
			tokens[COMMAND_TOKEN].value, "delete") == 0)) {
		/*
		 * delete <key> [<time>] [noreply]\r\n
		 *
		 * "DELETED\r\n" to indicate success
		 * "NOT_FOUND\r\n" to indicate that the item with this key was not
		 */
	} else if ((ntokens == 7 || ntokens == 8) && (strcmp(
			tokens[COMMAND_TOKEN].value, "cas") == 0)) {
		/*
		 * cas <key> <flags> <exptime> <bytes> <cas unqiue> [noreply]\r\n
		 * <data block>\r\n
		 *
		 * "STORED\r\n", to indicate success.
		 * "NOT_STORED\r\n" to indicate the data was not stored, but not
		 * "EXISTS\r\n" to indicate that the item you are trying to store with
		 * "NOT_FOUND\r\n" to indicate that the item you are trying to store
		 */
		c->flag.is_set_cmd = 1;
		c->storebytes = atol(tokens[BYTES_TOKEN].value);
		c->storebytes += 2; /* \r\n */
	} else if ((ntokens == 6 || ntokens == 7) && ((strcmp(
			tokens[COMMAND_TOKEN].value, "add") == 0) || (strcmp(
			tokens[COMMAND_TOKEN].value, "set") == 0) || (strcmp(
			tokens[COMMAND_TOKEN].value, "replace") == 0) || (strcmp(
			tokens[COMMAND_TOKEN].value, "prepend") == 0) || (strcmp(
			tokens[COMMAND_TOKEN].value, "append") == 0))) {
		/*
		 * <cmd> <key> <flags> <exptime> <bytes> [noreply]\r\n
		 * <data block>\r\n
		 *
		 * "STORED\r\n", to indicate success.
		 * "NOT_STORED\r\n" to indicate the data was not stored, but not
		 * "EXISTS\r\n" to indicate that the item you are trying to store with
		 * "NOT_FOUND\r\n" to indicate that the item you are trying to store
		 */
		c->flag.is_set_cmd = 1;
		c->storebytes = atol(tokens[BYTES_TOKEN].value);
		c->storebytes += 2; /* \r\n */
	} else if (ntokens >= 2 && (strcmp(tokens[COMMAND_TOKEN].value, "stats")
			== 0)) {
		/* END\r\n
		 */
		char tmp[128];
		out_string(c, "memcached agent v" VERSION);
		for (i = 0; i < matrixcnt; i++) {
			snprintf(tmp, 127, "matrix %d -> %s:%d, pool size %d", i + 1,
					matrixs[i].ip, matrixs[i].port, matrixs[i].used);
			out_string(c, tmp);
		}
		out_string(c, "END");
		skip = 1;
	} else if (ntokens == 2 && (strcmp(tokens[COMMAND_TOKEN].value, "quit")
			== 0)) {
		buffer_free(b);
		conn_close(c);
		return;
	} else if (ntokens == 2 && (strcmp(tokens[COMMAND_TOKEN].value, "version")
			== 0)) {
		out_string(c, "VERSION memcached agent v" VERSION);
		skip = 1;
	} else {
		out_string(c, "UNSUPPORTED COMMAND");
		skip = 1;
	}

	/* finish process commands */
	if (skip == 0) {
		/* append buffer to list */
		append_buffer_to_list(c->request, b);

		if (c->flag.is_get_cmd == 0) {
			if (tokens[ntokens - 2].value && strcmp(tokens[ntokens - 2].value,
					"noreply") == 0)
				c->flag.no_reply = 1;
			c->keycount = 1;
			c->keys = (char **) calloc(sizeof(char *), 1);
			if (c->keys == NULL) {
				fprintf(stderr, "%s: (%s.%d) SERVER OUT OF MEMORY\n",
						cur_ts_str, __FILE__, __LINE__);
				conn_close(c);
				return;
			}
			c->keys[0] = strdup(tokens[KEY_TOKEN].value);
		}
	} else {
		buffer_free(b);
	}

	i = p - c->line + 1;
	if (i < c->pos) {
		memmove(c->line, p + 1, c->pos - i);
		c->pos -= i;
	} else {
		c->pos = 0;
	}

	if (c->storebytes > 0) {
		if (c->pos > 0) {
			/* append more buffer to list */
			b = buffer_init_size(c->pos + 1);
			if (b == NULL) {
				fprintf(stderr, "%s: (%s.%d) SERVER OUT OF MEMORY\n",
						cur_ts_str, __FILE__, __LINE__);
				conn_close(c);
				return;
			}
			memcpy(b->ptr, c->line, c->pos);
			b->size = c->pos;
			c->storebytes -= b->size;
			append_buffer_to_list(c->request, b);
			c->pos = 0;
		}
		if (c->storebytes > 0)
			c->state = CLIENT_NREAD;
		else
			start_magent_transcation(c);
	} else {
		if (skip == 0)
			start_magent_transcation(c);
	}
}

/* drive machine of client connection */
static void drive_client(const int fd, const short which, void *arg) {
	conn *c;
	int r, toread;
	buffer *b;

	c = (conn *) arg;
	if (c == NULL)
		return;

	if (which & EV_READ) {
		/* get the byte counts of read */
		if (ioctl(c->cfd, FIONREAD, &toread) || toread == 0) {
			conn_close(c);
			return;
		}

		switch (c->state) {
		case CLIENT_TRANSCATION:
		case CLIENT_COMMAND:
			r = BUFFERLEN - c->pos;
			if (r > toread)
				r = toread;

			toread = read(c->cfd, c->line + c->pos, r);
			if ((toread <= 0) && (errno != EINTR && errno != EAGAIN)) {
				conn_close(c);
				return;
			}
			c->pos += toread;
			c->line[c->pos] = '\0';
			process_command(c);
			break;
		case CLIENT_NREAD:
			/* we are going to read */
			if (c->flag.is_set_cmd == 0) {
				fprintf(stderr,
						"%s: (%s.%d) WRONG STATE, SHOULD BE SET COMMAND\n",
						cur_ts_str, __FILE__, __LINE__);
				conn_close(c);
				return;
			}

			if (toread > c->storebytes)
				toread = c->storebytes;

			b = buffer_init_size(toread + 1);
			if (b == NULL) {
				fprintf(stderr, "%s: (%s.%d) SERVER OUT OF MEMORY\n",
						cur_ts_str, __FILE__, __LINE__);
				conn_close(c);
				return;
			}

			r = read(c->cfd, b->ptr, toread);
			if ((r <= 0) && (errno != EINTR && errno != EAGAIN)) {
				buffer_free(b);
				conn_close(c);
				return;
			}
			b->size = r;
			b->ptr[r] = '\0';

			/* append buffer to list */
			append_buffer_to_list(c->request, b);
			c->storebytes -= r;
			if (c->storebytes <= 0)
				start_magent_transcation(c);
			break;
		}
	} else if (which & EV_WRITE) {
		/* write to client */
		r = writev_list(c->cfd, c->response);
		if (r < 0) {
			conn_close(c);
			return;
		}

		if (c->response->first == NULL) {
			/* finish writing buffer to client
			 * switch back to reading from client
			 */
			event_del(&(c->ev));
			event_set(&(c->ev), c->cfd, EV_READ | EV_PERSIST, drive_client,
					(void *) c);
			event_add(&(c->ev), 0);
			c->ev_flags = EV_READ;
		}
	}
}

static void server_accept(const int fd, const short which, void *arg) {
	conn *c = NULL;
	int newfd, flags = 1;
	struct sockaddr_in s_in;
	socklen_t len = sizeof(s_in);
	struct linger ling = { 0, 0 };

	UNUSED(arg);
	UNUSED(which);

	memset(&s_in, 0, len);
	newfd = accept(fd, (struct sockaddr *) &s_in, &len);
	if (newfd < 0) {
		fprintf(stderr, "%s: (%s.%d) ACCEPT() FAILED\n", cur_ts_str, __FILE__,
				__LINE__);
		return;
	}

	if (curconns >= glob_config->maxconns) {
		/* out of connections */
		write(newfd, OUTOFCONN, sizeof(OUTOFCONN));
		close(newfd);
		return;
	}

	c = (struct conn *) calloc(sizeof(struct conn), 1);
	if (c == NULL) {
		fprintf(stderr, "%s: (%s.%d) OUT OF MEMORY FOR NEW CONNECTION\n",
				cur_ts_str, __FILE__, __LINE__);
		close(newfd);
		return;
	}
	c->request = list_init();
	c->response = list_init();
	c->cfd = newfd;
	curconns++;

	if (glob_config->verbose_mode)
		fprintf(stderr, "%s: (%s.%d) NEW CLIENT FD %d\n", cur_ts_str, __FILE__,
				__LINE__, c->cfd);

	fcntl(c->cfd, F_SETFL, fcntl(c->cfd, F_GETFL) | O_NONBLOCK);
	setsockopt(c->cfd, SOL_SOCKET, SO_LINGER, (void *) &ling, sizeof(ling));
	setsockopt(c->cfd, SOL_SOCKET, SO_KEEPALIVE, (void *) &flags, sizeof(flags));
	setsockopt(c->cfd, SOL_SOCKET, SO_REUSEADDR, (void *) &flags, sizeof(flags));

	/* setup client event handler */
	memset(&(c->ev), 0, sizeof(struct event));
	event_set(&(c->ev), c->cfd, EV_READ | EV_PERSIST, drive_client, (void *) c);
	event_add(&(c->ev), 0);
	c->ev_flags = EV_READ;

	return;
}

static void free_matrix(matrix *m) {
	int i;
	struct server *s;

	if (m == NULL)
		return;

	for (i = 0; i < m->used; i++) {
		s = m->pool[i];
		if (s->sfd > 0)
			close(s->sfd);
		list_free(s->request, 0);
		list_free(s->response, 0);
		free(s);
	}
	if (m->name)
		free(m->name);

	if (m->pool)
		free(m->pool);
	if (m->ip)
		free(m->ip);
}

static void server_exit(int sig) {
	int i;

	if (glob_config->verbose_mode)
		fprintf(stderr, "\nexiting\n");
	jlog(L_INFO, "\nexiting\n");

	UNUSED(sig);

	if (sockfd > 0)
		close(sockfd);

	free_ketama(ketama);
	free_ketama(backupkt);

	for (i = 0; i < matrixcnt; i++) {
		free_matrix(matrixs + i);
	}

	free(matrixs);

	for (i = 0; i < backupcnt; i++) {
		free_matrix(backups + i);
	}

	free(backups);

	config_free();

	exit(0);
}
#if 0
static void
server_socket_unix(void)
{
	struct linger ling = {0, 0};
	struct sockaddr_un addr;
	struct stat tstat;
	int flags = 1;
	int old_umask;

	if (socketpath == NULL)
	return;

	if ((unixfd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
		fprintf(stderr, "%s: (%s.%d) CAN NOT CREATE UNIX DOMAIN SOCKET", cur_ts_str, __FILE__, __LINE__);
		return;
	}

	fcntl(unixfd, F_SETFL, fcntl(unixfd, F_GETFL, 0) | O_NONBLOCK);

	/*
	 * Clean up a previous socket file if we left it around
	 */
	if (lstat(socketpath, &tstat) == 0) {
		if (S_ISSOCK(tstat.st_mode))
		unlink(socketpath);
	}

	setsockopt(unixfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
	setsockopt(unixfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
	setsockopt(unixfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));

	/*
	 * the memset call clears nonstandard fields in some impementations
	 * that otherwise mess things up.
	 */
	memset(&addr, 0, sizeof(addr));

	addr.sun_family = AF_UNIX;
	strcpy(addr.sun_path, socketpath);
	old_umask=umask( ~(0644&0777));
	if (bind(unixfd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
		jlog(L_ERR, " (%s.%d) bind errno = %d: %s\n", __FILE__, __LINE__, errno, strerror(errno));
		close(unixfd);
		unixfd = -1;
		umask(old_umask);
		return;
	}

	umask(old_umask);

	if (listen(unixfd, 512) == -1) {
		jlog(L_ERR, " (%s.%d) listen errno = %d: %s\n", __FILE__, __LINE__, errno, strerror(errno));
		close(unixfd);
		unixfd = -1;
	}
}
#endif
/*
 static void
 timer_service(const int fd, short which, void *arg)
 {
 struct timeval tv;

 cur_ts = time(NULL);
 strftime(cur_ts_str, 127, "%Y-%m-%d %H:%M:%S", localtime(&cur_ts));

 tv.tv_sec = 1; tv.tv_usec = 0; //check for every 1 seconds
 event_add(&ev_timer, &tv);
 }
 */

static void reload_config(const char *config_file) {
	int index;
	char **node;
	GError *err = NULL;
	GKeyFile *keyfile;
	keyfile = load_config(config_file);

	gsize node_length;
	node = g_key_file_get_string_list(keyfile, "General", "masters",
			&node_length, &err);

	if (err) {
		fprintf(stderr, "at least one master memcached server \n");
		exit(-1);
	}
	int j;
	matrixs_new = (struct matrix *) calloc(sizeof(struct matrix), node_length);
	for (index = 0; index < node_length; index++) {
		init_matrixs_item(node[index], matrixs_new + index);
		for (j = 0; j < matrixcnt; j++) {
			if (strcmp(matrixs[j].name, matrixs_new[index].name) == 0)//假如相等
			{
				matrixs_new[index].used = matrixs[j].used;
				matrixs_new[index].size = matrixs[j].size;
				matrixs_new[index].pool = matrixs[j].pool;
				break;
			}
		}
	}
	matrixcnt = node_length;
	matrixs = matrixs_new;
	g_strfreev(node);

	node = g_key_file_get_string_list(keyfile, "General", "backups",
			&node_length, &err);
	if (err) {
		//fprintf(stderr,"at least one master memcached server \n");
		//g_strfreev(node);
		return;
	}

	matrixs_new = (struct matrix *) calloc(sizeof(struct matrix), node_length);
	for (index = 0; index < node_length; index++) {
		init_matrixs_item(node[index], matrixs_new + index);
		for (j = 0; j < backupcnt; j++) {
			if (strcmp(backups[j].name, matrixs_new[index].name) == 0)//假如相等
			{
				matrixs_new[index].used = backups[j].used;
				matrixs_new[index].size = backups[j].size;
				matrixs_new[index].pool = backups[j].pool;
				break;
			}
		}
	}
	backups = matrixs_new;
	backupcnt = node_length;
	g_strfreev(node);

	g_key_file_free(keyfile);
	free_ketama(ketama);
	free_ketama(backupkt);
	init_ketama();

}

static void signal_cb(int fd, short event, void *arg) {
	struct event *signal = arg;

	switch (EVENT_SIGNAL(signal)) {
	case SIGINT:
		signal_shutdown = 1;
		break;
	case SIGTERM:
		signal_shutdown = 1;
		break;
	case SIGHUP:
		signal_reload = 1;
		jlog(L_INFO, "%s has reloaded \n", LOG_PROGRAMME);
		break;
	}
}

int main(int argc, char **argv) {
	char c;
	if (argc <= 2) {
		fprintf(stderr, "please usage ./lightredis -c mqagent.conf\n");
		exit(-1);
	}
	while (-1 != (c = getopt(argc, argv, "c:h:"))) {
		switch (c) {
		case 'c':
			config_file = (char *) malloc(strlen(optarg) + 1);
			sprintf(config_file, "%s", optarg);
			break;
		case 'h':
		case '?':
			printf("usage ./lightredis -c lightredis.conf");
			break;
		}
	}


	init_config(config_file);
	init_socket();
	event_init();
	//添加信号
	event_set(&signal_int, SIGINT, EV_SIGNAL | EV_PERSIST, signal_cb,
			&signal_int);
	event_add(&signal_int, NULL);

	event_set(&signal_term, SIGTERM, EV_SIGNAL | EV_PERSIST, signal_cb,
			&signal_term);
	event_add(&signal_term, NULL);

	event_set(&signal_hup, SIGHUP, EV_SIGNAL | EV_PERSIST, signal_cb,
			&signal_hup);
	event_add(&signal_hup, NULL);

	if (sockfd > 0) {
		if (glob_config->verbose_mode)
			fprintf(stderr, "memcached agent listen at port %d\n",
					glob_config->port);
		event_set(&ev_master, sockfd, EV_READ | EV_PERSIST, server_accept, NULL);
		event_add(&ev_master, 0);
	}

	struct timeval timeout;

	timeout.tv_sec = 1;
	timeout.tv_usec = 0;
	//每个一秒退出一次，检测是否关闭
	while (signal_shutdown == 0) {
		event_loopexit(&timeout);

		event_dispatch();
		if (signal_reload) {
			jlog(L_INFO, "reload!\n");
			signal_reload = 0;
			reload_config(config_file);
		}
	}

	server_exit(0);
	return 0;
}
