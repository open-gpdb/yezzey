#ifndef YEZZEY_GUCS_H
#define YEZZEY_GUCS_H

extern int yezzey_log_level;
extern int yezzey_ao_log_level;

extern bool use_gpg_crypto;

/* ----- STORAGE -----  */
extern char *storage_class;
extern int multipart_chunksize;
extern int multipart_threshold;

/* Y-PROXY */
extern char *yproxy_socket;

#endif /* YEZZEY_GUCS_H */