#ifndef __WAITFREE_PATCHING_H
#define __WAITFREE_PATCHING_H

#include <signal.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif
    
// #define SIGPATCH (SIGRTMIN+0)

typedef enum {
    WF_LOW = 0,
    WF_MEDIUM = 1,
    WF_HIGH = 2,
    WF_CRITICAL = 3,
} wf_thread_priority;

/*
 In order to use the wait free patching mechanism, you have to
 initialize and configure the library. This should be done at the beginning of your main function.

 wf_init will install an signal handler for SIGPATCH and spawn a thread
 that will coordinate the applying a patch when the signal was received[1].
 */
struct wf_configuration {
    int amount_priorities;
    int trigger_sleep_ms;
    // Some applications require some extra triggering to reach global
    // or local quiescence points. With these callbacks the library
    // issues such an application kicking.
    int (*trigger_global_quiescence)(int);
    int (*trigger_local_quiescence)(int);

    void (*thread_migrated)(int remaining);

    // OPTIONAL: Is called after patching is done.
    void (*patch_applied)(void);

    // OPTIONAL: Is called after all threads are migrated
    void (*patch_done)(void);
};

void wf_init(struct wf_configuration config);

bool wf_quiescence(void);

bool wf_local_quiescence(void);
bool wf_global_quiescence(void);

void wf_signal_patching(void);
void wf_trigger_patch(void);

//void wf_thread_birth_group(char *name, char *group);
void wf_thread_birth(char *name);
void wf_thread_death(void);

void wf_thread_set_priority(int priority);

void wf_thread_activate(void);
void wf_thread_deactivate(void);

int wf_get_local_thread_id();
void wf_set_patch(char *patch);
void wf_set_patch_method(int method);

void wf_log(char *fmt, ...);
double wf_time(void);

#ifdef __cplusplus
}
#endif

#endif
