#define _GNU_SOURCE 1
#include <stdbool.h>
#include <stdio.h>
#include <glob.h>
#include <stdarg.h>
#include <sys/epoll.h>
#include <pthread.h>
#include <dlfcn.h>
#include <stddef.h>
#include <errno.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <limits.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <link.h>
#include <elf.h>

//////////////////// COUNT VMA ////////////////////////
#include <regex.h>      
static void count_vmas(char *);
///////////////////////////////////////////////////////
//
#include "wf-userland.h"

static void *wf_plt_trampoline(char *name, void *func_ptr);


static struct wf_configuration wf_config;

#define log(...) do { fprintf(stderr, "wf-userland: "__VA_ARGS__); } while(0)
#define die(...) do { log("[ERROR] " __VA_ARGS__); exit(EXIT_FAILURE); } while(0)
#define die_perror(m, ...) do { perror(m); die(__VA_ARGS__); } while(0)

// WF_LOGFILE
static FILE *wf_log_file;
static void wf_log(char *fmt, ...) {
    if (wf_log_file) {
        va_list(args);
        va_start(args, fmt);
        vfprintf(wf_log_file, fmt, args);
        fflush(wf_log_file);
    }
}

// Returns the a timestamp in miliseconds. The first call zeroes the clock
static struct timespec wf_ts0;
static void wf_timestamp_reset(void) { // returns 0 seconds first time called
    clock_gettime(CLOCK_REALTIME, &wf_ts0);
}

static double wf_timestamp(void) { // returns 0 seconds first time called
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (ts.tv_sec - wf_ts0.tv_sec)*1000. + (ts.tv_nsec - wf_ts0.tv_nsec) / 1000000.;
}



////////////////////////////////////////////////////////////////
// Kernel Interface
static unsigned pagesize;
static void *addr_to_page(void *addr) {
    if (pagesize == 0) {
        pagesize = sysconf(_SC_PAGESIZE);
    }
    void *page = addr - ((uintptr_t) addr % pagesize);
    return page;
}

int wf_kernel_pin(void* start, void* end) {
    // Pin code as non shared for AS generations
    uintptr_t start_page = (uintptr_t)addr_to_page(start);
    uintptr_t end_page   = (uintptr_t)addr_to_page(end + pagesize - 1);

    int rc = syscall(1000, 4, start_page, end_page - start_page);
    log("memory pin [%p:+0x%lx]: rc=%d\n", (void*)start_page,
        end_page - start_page, rc);
    if (rc == -1)
        die_perror("wf_kernel_pin", "Could not unshare the text segment");
    
    return rc;
}

int wf_kernel_as_new(void) {
    int rc = syscall(1000, 0);
    // log("AS create: %d\n", rc);
    if (rc == -1)
        die_perror("wf_kernel_as_new", "Could not create a new address space");
    return rc;
}

int wf_kernel_as_switch(int as_id) {
    int rc = syscall(1000, 3, as_id);
    // log("AS switch: %d %d\n", as_id, rc);
    if (rc == -1)
        die_perror("wf_kernel_as_switch", "Could not migrate to the new address space");
    return rc;
}

int wf_kernel_as_delete(int as_id) {
    int rc = syscall(1000, 1, as_id);
    // log("address space delete(%d)=%d\n", as_id, rc);
    return rc;
}


////////////////////////////////////////////////////////////////
// Apply Patch
// Magic Names: ehdr, shstr
#define offset_to_ptr(offset) (((void*) (ehdr)) + ((int) (offset)))
#define shdr_from_idx(idx) offset_to_ptr(ehdr->e_shoff + ehdr->e_shentsize * idx);
#define section_name(shdr) (((char*) offset_to_ptr(shstr->sh_offset)) + shdr->sh_name)

extern void *_GLOBAL_OFFSET_TABLE_;

bool wf_relocate_calc(unsigned type,
                        /* input */  uintptr_t reloc_src, uintptr_t reloc_dst, uintptr_t reloc_addend,
                        /* output */ void **loc, uint64_t *val, char *size) {

    uintptr_t S = reloc_src, A = reloc_addend, P = reloc_dst;

#define is_pc32_rel(x) ((INT_MIN <= (intptr_t) (x)) && ((intptr_t) (x) <= INT_MAX))

    *loc = (void*) reloc_dst;
    switch (type) {
    case R_X86_64_NONE:
        return false;
    case R_X86_64_PC32:
    case R_X86_64_PLT32:
    case R_X86_64_GOTPCREL:
    case R_X86_64_REX_GOTPCRELX:
        *val = (uint64_t)(S + A - P);
        assert(is_pc32_rel(*val) && "Patch was loaded tooo far away");
        *size = 4;
        break;
    case R_X86_64_32S:
        *val = (uint64_t)((int32_t)S + A);
        *size = 4;
        break;
    case R_X86_64_32:
        *val = (uint64_t)((uint32_t)S + A);
        *size = 4;
        break;
    case R_X86_64_64:
        *val = (uint64_t) S + A;
        *size = 8;
        break;
    default:
        die("Unsupported relocation %d for source (0x%lx <- 0x%lx)\n",
            type, reloc_dst, reloc_src);
    }
    return true;
}

void *wf_find_symbol(char * name);

void wf_relocate(Elf64_Ehdr *ehdr, void *elf_end, Elf64_Shdr* shdr) {
    // Section Header String table
    Elf64_Shdr *shstr = shdr_from_idx(ehdr->e_shstrndx);
    
    Elf64_Shdr *shdr_symtab = shdr_from_idx(shdr->sh_link);
    Elf64_Sym *symbol_table = offset_to_ptr(shdr_symtab->sh_offset);
    
    Elf64_Shdr *shdr_strtab = shdr_from_idx(shdr_symtab->sh_link);
    char *strtab = offset_to_ptr(shdr_strtab->sh_offset);

    // Which section should be modified
    Elf64_Shdr * shdr_target = shdr_from_idx(shdr->sh_info);

    unsigned  rela_num = shdr->sh_size / shdr->sh_entsize;
    log("[%s] %d relocs for %s\n",
           section_name(shdr),
           rela_num,
           section_name(shdr_target));

    if (!strncmp(".rela.debug_", section_name(shdr), 12)) {
        log(".. debug relocations, skipping.\n");
        return;
    }

    Elf64_Rela * rela = offset_to_ptr(shdr->sh_offset);
    for (unsigned i = 0; i < rela_num; i++) {
        Elf64_Rela * rela = offset_to_ptr(shdr->sh_offset + i * shdr->sh_entsize);

        // Where to modify
        uintptr_t reloc_dst = (uintptr_t)  offset_to_ptr(shdr_target->sh_offset + rela->r_offset);

        // What should be written into that place
        Elf64_Sym * symbol = &symbol_table[ELF64_R_SYM(rela->r_info)];
        char *symbol_name = strtab + symbol->st_name;
        void *reloc_src;
        if (symbol->st_shndx != 0) {
            Elf64_Shdr * symbol_section = shdr_from_idx(symbol->st_shndx);
            if (ELF64_ST_TYPE(symbol->st_info) == STT_SECTION) {
                symbol_name = section_name(symbol_section);
                reloc_src = offset_to_ptr(symbol_section->sh_offset);
            } else {
                reloc_src = offset_to_ptr(symbol_section->sh_offset + symbol->st_value);
            }
        } else if (symbol_name && *symbol_name == 0) {
            log("Empty Symbol name... continue on this...\n");
            continue;
        } else { 
            // Find Name in original binary
            reloc_src = wf_find_symbol(symbol_name);
            if (!reloc_src) {
                reloc_src = dlsym(RTLD_DEFAULT, symbol_name);
                reloc_src = wf_plt_trampoline(symbol_name, reloc_src);
            }
            if (!reloc_src) {
                log("Probaly `%s' is a library function that was not called in original binary. We do not support this yet.\n", symbol_name);
                die("Could not find symbol %s.\n", symbol_name);
            }
        }

        // FIXME: log_debug
        // log("   %p+(%ld) -> %p (%s)\n", (void*)reloc_dst, rela->r_addend, reloc_src, symbol_name);

        void* loc;
        uint64_t val;
        char size;
        bool action = wf_relocate_calc(ELF64_R_TYPE(rela->r_info),
                                       (uintptr_t) reloc_src, reloc_dst, rela->r_addend,
                                       &loc, &val, &size);
        if (!action) continue;

        if (loc < (void*) ehdr || loc >= elf_end) {
			die("bad relocation 0x%p for symbol %s\n", loc, symbol_name);
		}

        // log("   *%p = 0x%lx [%d]\n", loc, val, size);

        if (size == 4) {
            *(uint32_t *) loc = val;
        } else if (size == 8) {
            *(uint64_t *) loc = val;
        } else
            die("Invalid relocation size");

    }
}

struct kpatch_patch_func {
	unsigned long new_addr;
	unsigned long new_size;
	unsigned long old_addr;
	unsigned long old_size;
	unsigned long sympos;
	char *name;
	char *objname;
};

struct kpatch_relocation {
	unsigned long dest;
	unsigned int type;
	int external;
	long addend;
	char *objname; /* object to which this rela applies to */
	struct kpatch_symbol *ksym;
};

struct kpatch_symbol {
	unsigned long src;
	unsigned long sympos;
	unsigned char bind, type;
	char *name;
	char *objname; /* object to which this sym belongs */
};

struct kpatch_patch_dynrela {
	unsigned long dest;
	unsigned long src;
	unsigned long type;
	unsigned long sympos;
	char *name;
	char *objname;
	int external;
	long addend;
};

struct kpatch_pre_patch_callback {
	int (*callback)(void *obj);
	char *objname;
};
struct kpatch_post_patch_callback {
	void (*callback)(void *obj);
	char *objname;
};
struct kpatch_pre_unpatch_callback {
	void (*callback)(void *obj);
	char *objname;
};
struct kpatch_post_unpatch_callback {
	void (*callback)(void *obj);
	char *objname;
};

static void *wf_vspace_end = NULL;
static void *wf_vspace_bump_ptr = NULL;

void * wf_vspace_reservere(uintptr_t bytes) {
    uintptr_t size = ((bytes + 0xfff) & (~((uintptr_t) 0xfff)));
    wf_vspace_bump_ptr -= size;
    log("vspace %p (%lx %lx)\n", wf_vspace_bump_ptr, bytes, size);
    return wf_vspace_bump_ptr;
}


static Elf64_Ehdr * wf_load_elf(char *filename, bool close_to_binary, void **elf_end) {
    int fd = open(filename, O_RDONLY);
    if (!fd) die_perror("open", "Could not open patch file: %s", filename);

    // Mapt the whole file
    struct stat size;
    if (fstat(fd, &size) == -1) die_perror("fstat", "Could not determine size: %s\n", filename);

    void *hint = 0;
    if (close_to_binary) {
        hint = wf_vspace_reservere(size.st_size);
    }
    void *elf_start = mmap(hint, size.st_size, PROT_WRITE, MAP_PRIVATE, fd, 0);
    if (elf_start == MAP_FAILED) die_perror("mmap", "Could not map patch: %s", filename);
    *elf_end = elf_start + size.st_size;

    // 1. Find Tables
    Elf64_Ehdr *ehdr = elf_start;

	if (strncmp((const char *)ehdr->e_ident, "\177ELF", 4) != 0) {
		die("Patch is not an ELF file");
	}
    if (ehdr->e_ident[EI_CLASS] != ELFCLASS64) {
        die("Patch is not an 64-Bit ELF");
    }
    return ehdr;
}

static void wf_unload_elf(Elf64_Ehdr *ehdr, void *elf_end) {
    munmap(ehdr, (uintptr_t) elf_end - (uintptr_t) ehdr);
}


struct wf_symbol {
    char * name;
    void * addr;
};

static struct wf_symbol *wf_symbols = NULL;
static unsigned wf_symbol_count;


static int dl_iterate_cb_stop;
static int
dl_iterate_cb(struct dl_phdr_info *info, size_t size, void *data) {
    Elf64_Ehdr *ehdr = (Elf64_Ehdr *) data;
    // We only process the main binary
    if (dl_iterate_cb_stop) return 0;
    dl_iterate_cb_stop = 1;

    // Section Header String table
    Elf64_Shdr *shstr = shdr_from_idx(ehdr->e_shstrndx);

    // Allocate enough space for all symbols
    unsigned symbols_max = 0;
    for (unsigned i = 0; i < ehdr->e_shnum; i++) {
        Elf64_Shdr * shdr = shdr_from_idx(i);
        if (shdr->sh_type == SHT_SYMTAB) {
            Elf64_Sym *symbol_table = offset_to_ptr(shdr->sh_offset);
            unsigned  symbol_count = shdr->sh_size / shdr->sh_entsize;
            symbols_max += symbol_count;
        }
    }
    wf_symbols = malloc(sizeof(struct wf_symbol) * symbols_max);
    if (!wf_symbols) die_perror("malloc", "could not allocate space for symbols");

    // Pin all the executable sections
    for (int j = 0; j < info->dlpi_phnum; j++) {
        const Elf64_Phdr *phdr = &info->dlpi_phdr[j];
        if (phdr->p_type != PT_LOAD) continue;
        void *seg_vstart = (void *) (info->dlpi_addr + phdr->p_vaddr);
        void *seg_vend = seg_vstart + phdr->p_memsz;

        if (phdr->p_flags & PF_X) { // Executable segment
            wf_kernel_pin(seg_vstart, seg_vend);
        }
    }

    // Initialize our patch bumping allocator before the actual
    // binary. We need this space to allocate PLT entries
    wf_vspace_end = (void*) ((uintptr_t) info->dlpi_addr & ~((uintptr_t) 0x1ff)) - 0x1000*1024;
    assert(((uintptr_t)wf_vspace_end & 0x1ff) == 0 && "Not page alinged");
    wf_vspace_bump_ptr = wf_vspace_end;

    // Find all Symbols in ELF
    for (unsigned i = 0; i < ehdr->e_shnum; i++) {
        Elf64_Shdr * shdr = shdr_from_idx(i);
        if (shdr->sh_type == SHT_SYMTAB) {
            Elf64_Sym *symbol_table = offset_to_ptr(shdr->sh_offset);
            unsigned  symbol_count = shdr->sh_size / shdr->sh_entsize;

            Elf64_Shdr *shdr_strtab = shdr_from_idx(shdr->sh_link);
            char *strtab = offset_to_ptr(shdr_strtab->sh_offset);

            for (unsigned s = 0; s < symbol_count; s++) {
                int sym_type = ELF32_ST_TYPE(symbol_table[s].st_info);
                if (sym_type != STT_FUNC && sym_type != STT_OBJECT)
                    continue;

                char *sym_name = strtab + symbol_table[s].st_name;
                ptrdiff_t sym_value = symbol_table[s].st_value;
                void *sym_addr = (void*)info->dlpi_addr + sym_value;

                wf_symbols[wf_symbol_count].name = strdup(sym_name);
                wf_symbols[wf_symbol_count].addr = sym_addr;
                wf_symbol_count += 1;
                // log("found %s @ %p (+0x%lx)\n", sym_name, sym_addr, sym_value);
            }
        }
    }
    return 0;
}


void wf_load_symbols(char *filename) {
    void * elf_end;
    Elf64_Ehdr * ehdr = wf_load_elf(filename, /* I don't care where */ 0, &elf_end);

    dl_iterate_cb_stop = false;
    dl_iterate_phdr(dl_iterate_cb, ehdr);

    wf_unload_elf(ehdr, elf_end);
}

void *wf_find_symbol(char * name) {
    for (unsigned i = 0; i < wf_symbol_count; i++) {
        if (strcmp(name, wf_symbols[i].name) == 0) {
            return wf_symbols[i].addr;
        }
    }
    
    return 0;
}

static int wf_malloc_space = 0;
static void *wf_malloc_ptr = 0;

void *wf_malloc_near(unsigned char size) {
    if (wf_malloc_space < size) {
        wf_malloc_ptr = wf_vspace_reservere(4096);
        {
            void *x = mmap(wf_malloc_ptr, 4096,
                           PROT_READ | PROT_WRITE | PROT_EXEC,
                           MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (x != wf_malloc_ptr) die_perror("mmap", "wf_malloc_near\n");
        }
        wf_malloc_space = 4096;
    }

    void * ret = wf_malloc_ptr;
    wf_malloc_ptr   += size;
    wf_malloc_space -= size;
    return ret;
}

static void *wf_plt_trampoline(char *name, void *func_ptr) {
    struct plt_entry {
        void *target;
        char jmpq[6];
    };
    struct plt_entry *plt = wf_malloc_near(sizeof(struct plt_entry));

    // jmp *%rip(-6)
    plt->target = func_ptr;
    plt->jmpq[0] = 0xff;
    plt->jmpq[1] = 0x25;
    *((uint32_t *)&plt->jmpq[2]) = -6 - 8;
    log("    PLT %s == %p (%p)\n", name, func_ptr, &plt->jmpq);

    return &plt->jmpq;
}

void wf_load_patch_from_file(char *filename) {
    void * elf_end;
    Elf64_Ehdr * ehdr = wf_load_elf(filename, /* close */ 1, &elf_end);

    int rc = mprotect(ehdr, elf_end - (void*)ehdr, PROT_WRITE | PROT_EXEC | PROT_READ);
    if (rc == -1) die_perror("mprotect", "Could not mprotect patch");


    Elf64_Shdr *shstr = shdr_from_idx(ehdr->e_shstrndx);

    // Extract all related sections from patch file
    Elf64_Shdr *kpatch_strings = NULL, *kpatch_funcs = NULL, *kpatch_relocations = NULL, *kpatch_symbols=NULL;

    for (unsigned i = 0; i < ehdr->e_shnum; i++) {
        Elf64_Shdr * shdr = shdr_from_idx(i);

        // Name of section
        char *name = section_name(shdr);

        // Perform relocations
        if (shdr->sh_type == SHT_RELA) {
            wf_relocate(ehdr, elf_end, shdr);
        }

        if (strcmp(name, ".kpatch.strings") == 0)
            kpatch_strings = shdr;
        else if (strcmp(name, ".kpatch.funcs") == 0)
            kpatch_funcs = shdr;
        else if (strcmp(name, ".kpatch.relocations") == 0)
            kpatch_relocations = shdr;
        else if (strcmp(name, ".kpatch.symbols") == 0)
            kpatch_symbols = shdr;
    }

    // Iterate over all symbols
    // printf("%d sections\n", ehdr->e_shnum);

    // Extract arrays
    struct kpatch_patch_func *funcs = offset_to_ptr(kpatch_funcs->sh_offset);
    int funcs_count = kpatch_funcs->sh_size / sizeof(struct kpatch_patch_func);
    assert(funcs_count * sizeof(struct kpatch_patch_func) == kpatch_funcs->sh_size);

    struct kpatch_symbol *symbols = offset_to_ptr(kpatch_symbols->sh_offset);
    int symbols_count = kpatch_symbols->sh_size / sizeof(struct kpatch_symbol);
    assert(symbols_count * sizeof(struct kpatch_symbol) == kpatch_symbols->sh_size);

    struct kpatch_relocation *relocations = offset_to_ptr(kpatch_relocations->sh_offset);
    int relocations_count = kpatch_relocations->sh_size / sizeof(struct kpatch_relocation);
    assert(relocations_count * sizeof(struct kpatch_relocation) == kpatch_relocations->sh_size);

    for (unsigned f = 0; f < funcs_count; f++) {
        log ("kpatch_func: name:%s objname:%s new: %p\n",
             funcs[f].name, funcs[f].objname, (void*) funcs[f].new_addr);

        funcs[f].old_addr = (uintptr_t) wf_find_symbol(funcs[f].name);
        // printf("PATCH %p -> %p\n", funcs[f].old_addr, funcs[f].new_addr);
        // Insert a call to the new function
        char * jumpsite = (char *)funcs[f].old_addr;
        void* page = addr_to_page(jumpsite);
        int rc = mprotect(page, pagesize, PROT_WRITE | PROT_EXEC | PROT_READ);
        
        if (rc == -1)
            die_perror("mprotect", "Cannot patch callsite at %p\n", page);
        
        *jumpsite = 0xe9; // jmp == 0xe9 OF OF OF OF
        *(int32_t*)(jumpsite + 1) = funcs[f].new_addr - 5 -funcs[f].old_addr;
        mprotect(page, pagesize, PROT_EXEC |PROT_READ);
    }

    // We know that ksyms are duplicated.
    // for (unsigned k = 0; k < symbols_count; k++) {
    //     printf("ksym: %s\n", symbols[k].name);
    // }

    // Relocations
    for (unsigned r = 0; r < relocations_count; r++) {
        struct kpatch_relocation *rela = &relocations[r];
        struct kpatch_symbol *ksym = rela->ksym;

        log("  reloc: [%s/%s pos=%ld, type=%d], ext=%d, type=%d, add=%d *%p\n",
            ksym->objname, ksym->name, ksym->sympos, ksym->type,
            rela->external, rela->type, rela->addend, (void*)rela->dest);

        uintptr_t reloc_src;
        if (rela->external) {
            reloc_src = dlsym(RTLD_DEFAULT, ksym->name);
            if (!reloc_src)
                die("Library symbol %s not found", ksym->name);
        } else {
            reloc_src = wf_find_symbol(ksym->name);
            log("    local %s == %p\n", ksym->name, reloc_src);
            if (!reloc_src) {
                die("Could not find symbol %s in original binary\n",
                    ksym->name);
            }
        }

        if (rela->type == R_X86_64_PLT32) {
            if (ksym->sympos == 0) {
                // Allocate space for trampolines
                ksym->sympos = (uintptr_t)wf_plt_trampoline(ksym->name, reloc_src);
            }
            reloc_src = ksym->sympos;
        } else if (rela->type == R_X86_64_GOTPCREL || rela->type == R_X86_64_REX_GOTPCRELX) {
            if (ksym->sympos == 0) {
                struct got_entry {
                    void *target;
                };
                struct got_entry *got = wf_malloc_near(sizeof(struct got_entry));
                ksym->sympos = (uintptr_t)got;
                got->target = reloc_src;
                log("    GOT %s == %p (%p)\n", ksym->name, reloc_src, got);
            }
            reloc_src = ksym->sympos;
        } else if (rela->type == R_X86_64_PC32
                   || rela->type == R_X86_64_64) {
            log("    direct %s == %p\n", ksym->name, reloc_src);
        } else {
            die("Unsupported relocation type %d for library name %s\n",
                rela->type, ksym->name);
        }

        void* loc;
        uint64_t val;
        char size;

        bool action = wf_relocate_calc(
            rela->type,
            (uintptr_t) reloc_src, rela->dest, rela->addend,
            &loc, &val, &size
            );
        if (!action) continue;
        // printf("%p %p %d\n", ksym_addr, rela->dest, rela->addend);
        // printf("PATCH *%p[%d] = %d\n", loc, size, val);

        if (size == 4) {
            *(uint32_t *) loc = val;
        } else if (size == 8) {
            *(uint64_t *) loc = val;
        } else
            die("Invalid relocation size");

        // Fixme OLD section

        rela->dest = 0;
    }

    for (unsigned r = 0; r < relocations_count; r++) {
        if (relocations[r].dest == 0) continue; // already handled

        printf("kpatch_relocation: name:%s/%s objname:%s type=%d, external=%d, *%p = ... (SHOULD NOT HAPPEN)\n",
               relocations[r].ksym->objname, relocations[r].ksym->name,
               relocations[r].objname, relocations[r].type, relocations[r].external,
               (void *)relocations[r].dest);
        // Fixme OLD section
        assert(false && "All relocations should have been handleded above");
    }

}



////////////////////////////////////////////////////////////////
// Patching Thread and API
static pthread_t wf_patch_thread;
static pthread_cond_t wf_cond_initiate;

/* Environment Variables - Start */
// WF_GLOBAL
static bool wf_is_global_quiescence;

// WF_GROUP
static bool wf_is_group_quiescence;
static char *wf_group_quiescence;

// WF_PATCH_ONLY_ACTIVE
static bool wf_is_patch_only_active_threads;
/* Environment Variables - End */
int wf_amount_priorities = 0;
int wf_trigger_sleep_ms = -1;
int wf_log_quiescence_priority = 0;

int wf_every_action_delay_ms = 0;

struct patch_measurement {
    double latency_as;
    double latency_as_time;
    double latency_migration;
    double latency_migration_time;
    int pte_size_kB;
};
struct patch_measurement *latencies = NULL;
int page_table_size(int);

double calculate_time(struct timespec *start) {
    return start->tv_sec * 1000. + start->tv_nsec / 1000000.;
}

double calculate_latency(struct timespec *start, struct timespec *end) {
    return (end->tv_sec - start->tv_sec) * 1000. + (end->tv_nsec - start->tv_nsec) / 1000000.;
}



static double wf_timestamp_start_quiescence;

/* Stores the current state of the thread.
 *
 *  Transitations that will have an effect (all other are irgnoed):
 *  ---------
 *  | Start |
 *  ---------
 *     |
 *    \ /
 * ---------
 * | Birth |--------------------------------
 * ---------                               |
 *    |                                    |
 *   \ /                                  \ /
 * ---------------   -------------     ---------
 * | Deactivated |<->| Activated |---->| Death |
 * ---------------   -------------     ---------
 *      |                                 /\
 *      |                                 |
 *      -----------------------------------
 * Start -> Birth
 * Birth -> Death
 * Birth -> Deactivated
 * Deactivated -> Activated
 * Deactivated -> Death
 * Activated -> Deactivated
 * Activated -> Death
 * */
typedef enum {
    WF_LOCAL_THREAD_STATE_START,
    WF_LOCAL_THREAD_STATE_BIRTH,
    WF_LOCAL_THREAD_STATE_ACTIVATED,
    WF_LOCAL_THREAD_STATE_DEACTIVATED,
    WF_LOCAL_THREAD_STATE_DEATH,
} wf_local_thread_state_t;

typedef struct {
    char *name;

    pthread_mutex_t mutex;
    pthread_cond_t cond_from_threads;
    pthread_cond_t cond_to_threads;

    volatile int born_threads;
    volatile int active_threads;
    volatile int migrated_threads;

    volatile int as;
    volatile int target_generation;
    volatile int previous_as;
} wf_group;

typedef struct {
  int id;
  pthread_t pthread_id;

  wf_group *group; // Point to a group (group shared between threads)

  char *name;
  volatile wf_local_thread_state_t state;

  volatile int current_generation;
  volatile int current_as;

  volatile bool in_global_quiescence;

  volatile int external_priority;
} wf_thread;


static volatile wf_group wf_master_group; // Every thread updates this

static __thread wf_thread wf_local_thread = {
    .state = WF_LOCAL_THREAD_STATE_START,
    .in_global_quiescence = false,
    .external_priority = -1
};
pthread_mutex_t wf_all_threads_mutex;
static wf_thread *wf_all_threads[1000] = { NULL };
static volatile int wf_total_threads = 0;

static volatile int thread_unique_id_counter = 0; // A global counter that only increments. Used to get a unique  wf_local_thread_id to access threads_to_wakeup

// WF_GROUP
static pthread_mutex_t wf_mutex_groups;
static volatile wf_group wf_groups[20];
static volatile int wf_groups_size = 0;


// Current WF_STATE
typedef enum {
    IDLE,
    GLOBAL_QUIESCENCE,
    LOCAL_QUIESCENCE,
} wf_state_t;
static volatile wf_state_t wf_state;

bool wf_is_quiescence() {
    return wf_state != IDLE;
}

int msleep(long msec)
{
    if (msec <= 0)
        return -1;

    struct timespec ts;
    int res;

    if (msec < 0)
    {
        errno = EINVAL;
        return -1;
    }

    ts.tv_sec = msec / 1000;
    ts.tv_nsec = (msec % 1000) * 1000000;

    do {
        res = nanosleep(&ts, &ts);
    } while (res && errno == EINTR);

    return res;
}

int wf_get_local_thread_id() {
    return wf_local_thread.id;
}

int wf_amount_patching_threads() {
    if (wf_is_patch_only_active_threads) {
        return wf_local_thread.group->active_threads;
    } else {
        return wf_local_thread.group->born_threads;
    }
}

wf_group *wf_find_group(char *group) {
    pthread_mutex_lock(&wf_mutex_groups);
    for (int i = 0; i < wf_groups_size; i++) {
        if (strcmp(group, wf_groups[i].name) == 0) {
            pthread_mutex_unlock(&wf_mutex_groups);
            return &wf_groups[i];
        }
    }
    pthread_mutex_unlock(&wf_mutex_groups);
    return NULL;
}

void wf_set_group(char *group) {
    if (wf_local_thread.group != NULL)
        return;

    // Use master group if:
    // - no group is set (group == NULL)
    // - when using global quiescence (wf_is_global_quiescence)
    // - When no group quiescence is used
    if (group == NULL || wf_is_global_quiescence || wf_group_quiescence == NULL)
        wf_local_thread.group = &wf_master_group;
    
    if (wf_local_thread.group == NULL)
         wf_local_thread.group = wf_find_group(group);
    
    if (wf_local_thread.group == NULL) {
        pthread_mutex_lock(&wf_mutex_groups);
    
        wf_groups[wf_groups_size].name = group;
        wf_groups[wf_groups_size].born_threads = 0;
        wf_groups[wf_groups_size].active_threads = 0;
        wf_groups[wf_groups_size].migrated_threads = 0;
        wf_groups[wf_groups_size].target_generation = 0;
        
        wf_local_thread.group = &wf_groups[wf_groups_size];
        wf_groups_size++;

        
        pthread_mutex_unlock(&wf_mutex_groups);
    }
}

// WF_PATCH_QUEUE
char *wf_patch_queue = NULL;

bool wf_load_patch(void) {
    char *patch = NULL;
    
    if (wf_patch_queue && *wf_patch_queue) {
        double time_patch_start = wf_timestamp();

        char *patch_stack = wf_patch_queue;
        char *p = strchr(wf_patch_queue, ';');
        if (p) { *p = 0; wf_patch_queue = p + 1;}
        else   { wf_patch_queue = 0; };

        char *patch_stack_cpy = strdup(patch_stack);
        log("applying patch: %s\n", patch_stack);
        char *saveptr = NULL;
        char *patch;
        p = patch_stack;
        while (patch = strtok_r(p, ",", &saveptr)) {
            p = NULL;
            if (strchr(patch, '*')){
                log("loading patch glob: %s\n", patch);
                glob_t globbuf;
                if (glob(patch, 0, NULL, &globbuf) != 0) {
                    die("glob failed: %s\n", patch);
                }
                for (unsigned i = 0; i < globbuf.gl_pathc; i++) {
                    log("loading patch: %s\n", globbuf.gl_pathv[i]);
                    wf_load_patch_from_file(globbuf.gl_pathv[i]);
                }
                globfree(&globbuf);
            } else {
                log("loading patch: %s\n", patch);
                wf_load_patch_from_file(patch);
            }
        }

        wf_log("- [patched, %.4f, \"%s\", \"%s\"]\n",
               wf_timestamp() - time_patch_start, patch_stack_cpy, wf_local_thread.group->name);
        free(patch_stack_cpy);

        if (wf_config.patch_applied)
            wf_config.patch_applied();

        return true;
    }
    return false;
}

static void wf_initiate_patching(void);

static void wf_sigpatch_handler(int sig) {
    pthread_cond_signal(&wf_cond_initiate);
}

static int wf_config_get(char * name, int default_value) {
    char *env = getenv(name);
    if (!env) return default_value;
    char *ptr;
    long ret;
    ret = strtol(env, &ptr, 10);
    if (!ptr || *ptr != '\0') die("invalid env config %s: %s", name, env);
    return (int) ret;
}

static double wf_config_get_double(char * name, double default_value) {
    char *env = getenv(name);
    if (!env) return default_value;
    char *ptr;
    double ret;
    ret = strtod(env, &ptr);
    if (!ptr || *ptr != '\0') die("invalid env config %s: %s", name, env);
    return ret;
}

double time_diff(struct timespec now, struct timespec future) {
    return ( future.tv_sec - now.tv_sec )
        +  ( future.tv_nsec - now.tv_nsec ) / 1E9;
}




// START FIFO
// FIFO original source code:
// Post: https://osg.tuhh.de/Advent/12-postbox/
// https://collaborating.tuhh.de/e-exk4/advent/-/blob/solution_12/12-postbox/postbox.c
// and https://collaborating.tuhh.de/e-exk4/advent/-/blob/solution_12/12-postbox/fifo.c
static void epoll_add(int epoll_fd, int fd, int events) {
    struct epoll_event ev;
    ev.events   = events;
    ev.data.fd  = fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        die("epoll_ctl: activate");
    }
}

static void epoll_del(int epoll_fd, int fd) {
    if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL) == -1) {
        die("epoll_ctl: reset");
    }
}

static int open_fifo(char *fifo) {
    int fd = open(fifo, O_RDONLY|O_NONBLOCK);
    if (fd < 0) die("open/fifo");
    return fd;
}

static int fifo_prepare(char *fifo, int epoll_fd) {
    int rc = unlink(fifo);
    if (rc < 0 && errno != ENOENT) die("unlink/fifo");
    rc = mknod(fifo, 0666 | S_IFIFO, 0);
    if (rc < 0) die("mknod/fifo");

    int fifo_fd = open_fifo(fifo);
    epoll_add(epoll_fd, fifo_fd, EPOLLIN);

    return fifo_fd;
}


static void* wf_patch_thread_entry(void *arg) {
    char *fifo = getenv("WF_PATCH_TRIGGER_FIFO");
    if (fifo == NULL) {
        // Disable patching...
        wf_log("!!! PATCHING DISABLED !!!\n");
        return;
    }
    
    wf_set_group(wf_group_quiescence);

    int epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) die("epoll_create");

    int fifo_fd = fifo_prepare(fifo, epoll_fd);
    
    // REDIS SPECIFICS:
    int latency_counter = 0;
    struct timespec start;
    struct timespec end;
    double latency_create_ms = -1;
    double latency_create_time = -1;
    double latency_migrate_ms = -1;
    double latency_migrate_time = -1;
    // ------------------

    for(;;) {
        struct epoll_event event[1];
        int nfds = epoll_wait(epoll_fd, event, 1, -1);
        int fd = event[0].data.fd;
        if (fd == fifo_fd) {
            static char buf[128];
            if (event[0].events & EPOLLIN) {
                int len = read(fifo_fd, buf, sizeof(buf));
                if (len < 0) die("read/fifo");
                if (len == 0) goto close;
                while (len > 1 && buf[len-1] == '\n') len --;
                buf[len] = 0;
                int pte_size_kB = page_table_size(latency_counter);
                if (strcmp(buf, "1") == 0) {
                    wf_initiate_patching();
                } else if (strcmp(buf, "2") == 0) {
                    // New AS every X seconds..
                    for(;;) {
                        double time_start_kernel_as_new = wf_timestamp();
                        wf_kernel_as_new();
                        wf_log("- [address-space-new, %.4f, \"%s\"]\n", wf_timestamp() - time_start_kernel_as_new, wf_local_thread.group->name);
                         msleep(wf_every_action_delay_ms);
                    }
                } else if (strcmp(buf, "3") == 0) {
                    // New AS + Migrate every X seconds..
                    for(;;) {
                        // Create
                        double time_start_kernel_as_new = wf_timestamp();
                        int as = wf_kernel_as_new();
                        wf_log("- [address-space-new, %.4f, \"%s\"]\n", wf_timestamp() - time_start_kernel_as_new, wf_local_thread.group->name);
                        
                        // Switch
                        double time_start_kernel_as_switch = wf_timestamp();
                        wf_kernel_as_switch(as);
                        wf_log("- [address-space-switch, %.4f]\n", wf_timestamp() - time_start_kernel_as_switch);
                        msleep(wf_every_action_delay_ms);
                    }
                } else if (strcmp(buf, "4") == 0) {
                    // Create new AS once and switch between old and new all the time..
                    int old_as = syscall(1000, 2);
                    int new_as = wf_kernel_as_new();
                    int current_as = old_as;
                    for (;;) {
                        double time_start_kernel_as_switch = wf_timestamp();
                        if (old_as == current_as) {
                            wf_kernel_as_switch(new_as);
                            current_as = new_as;
                        } else {
                            wf_kernel_as_switch(old_as);
                            current_as = old_as;
                        }
                        wf_log("- [address-space-switch, %.4f]\n", wf_timestamp() - time_start_kernel_as_switch);
                        msleep(wf_every_action_delay_ms);
                    }
                // REDIS EXPERIMENTS:
                } else if (strcmp(buf, "5") == 0) {
                    // Create new AS
                    clock_gettime(CLOCK_REALTIME, &start);
                    int as = wf_kernel_as_new(); 
                    clock_gettime(CLOCK_REALTIME, &end);

                    latency_create_ms = calculate_latency(&start, &end);
                    latency_create_time = calculate_time(&start);
                } else if (strcmp(buf, "6") == 0) {
                    // Fork
                    clock_gettime(CLOCK_REALTIME, &start);
                    if (fork() == 0) {
                        exit(0);
                    }
                    clock_gettime(CLOCK_REALTIME, &end);

                    latency_create_ms = calculate_latency(&start, &end);
                    latency_create_time = calculate_time(&start);
                } else if (strcmp(buf, "7") == 0) {
                    // Write results
                    char *logfile = getenv("WF_LOGFILE");
                    char *resultfile = malloc(strlen(logfile) + strlen(".csv"));
                    strcat(resultfile, logfile);
                    strcat(resultfile, ".csv");

                    FILE *resultfile_fp = fopen(resultfile, "w+");
                    fprintf(resultfile_fp, "latency_kernel_ms, latency_kernel_time, latency_switch_ms, latency_switch_time,pte_size_kB\n");
                    for (int i = 0; i < latency_counter; i++) {
                        struct patch_measurement m = latencies[i];
                        fprintf(resultfile_fp, "%f,%f,%f,%f,%d\n", m.latency_as, m.latency_as_time, m.latency_migration, m.latency_migration_time, m.pte_size_kB);
                    }
                fflush(resultfile_fp);
                fclose(resultfile_fp);
                }
                latencies[latency_counter] = (struct patch_measurement) {.latency_as = latency_create_ms, .latency_as_time = latency_create_time, .latency_migration = latency_migrate_ms, .latency_migration_time = latency_migrate_time, .pte_size_kB = pte_size_kB};
                latency_counter++;
            } else if (event[0].events & EPOLLHUP) {
close:
                epoll_del(epoll_fd, fifo_fd);
                close(fifo_fd);

                fifo_fd = open_fifo(fifo);
                epoll_add(epoll_fd, fifo_fd, EPOLLIN);
            }
        }
    }
    return NULL;
}

// END FIFO

static void wf_log_reach_quiescence_point() {
    wf_log("- [reach-quiescence-point, %.4f, \"%s\", \"%s\", %d]\n",
            wf_timestamp() - wf_timestamp_start_quiescence, wf_local_thread.name, wf_local_thread.group->name, wf_local_thread.id);
}

static void wf_log_migrated() {
    wf_log("- [migrated, %.4f, %d, \"%s\", \"%s\", %d]\n",
            wf_timestamp() - wf_timestamp_start_quiescence, wf_local_thread.current_as, wf_local_thread.name, wf_local_thread.group->name, wf_local_thread.id);
}

static void wf_initiate_patching(void) {
    if (!wf_is_global_quiescence && wf_config_get("WF_LOCAL_SINGLE_AS", 0) != 0) {
        // Do nothing; Local migration is handeled by wf_thread_birth.
        return;
    }

    static int first = 0;
    if (!first) first = 1;
    else wf_log("---\n");
   
    // Print statistics about the current VMAs BEFORE time measurement starts 
    count_vmas("before");

    // Reset the time
    wf_timestamp_reset();
    
    wf_log("- [apply, %ld.%09ld, %s, %d, \"%s\"]\n",
           wf_ts0.tv_sec,
           wf_ts0.tv_nsec,
           wf_is_global_quiescence ? "global" : "local",
           wf_amount_patching_threads(),
           wf_local_thread.group->name
        );


    wf_state = IDLE;

    if (wf_is_global_quiescence) {
        pthread_mutex_lock(&wf_local_thread.group->mutex);
        wf_local_thread.group->migrated_threads = 0;

        wf_timestamp_start_quiescence = wf_timestamp();
        wf_state = GLOBAL_QUIESCENCE;
        wf_local_thread.group->target_generation++;

        // Some Applications need a trigger to reach global quiescence
        if (wf_config.trigger_global_quiescence) {
            int trigger_counter = 0;
            while (wf_local_thread.group->migrated_threads < wf_amount_patching_threads()) {
                pthread_mutex_unlock(&wf_local_thread.group->mutex);
                if (wf_config.trigger_global_quiescence(trigger_counter++) == 0) {
                    pthread_mutex_lock(&wf_local_thread.group->mutex);
                    break;
                }
                msleep(wf_trigger_sleep_ms);
                pthread_mutex_lock(&wf_local_thread.group->mutex);
            }
        }

        while (wf_local_thread.group->migrated_threads < wf_amount_patching_threads())
            pthread_cond_wait(&wf_local_thread.group->cond_from_threads, &wf_local_thread.group->mutex);
        ////////////////////////////////////////////////////////////////
        wf_log("- [quiescence, %.4f, \"%s\"]\n", wf_timestamp(), wf_local_thread.group->name);

        // Load and Apply the patch
        wf_load_patch();

        wf_state = IDLE;

        ////////////////////////////////////////////////////////////////
        // Let's leave the global quiescence point
        wf_log("- [finished, %.4f, \"%s\"]\n", wf_timestamp(), wf_local_thread.group->name);
        pthread_cond_broadcast(&wf_local_thread.group->cond_to_threads); // Wakeup all sleeping threads
        ////////////////////////////////////////////////////////////////
        pthread_mutex_unlock(&wf_local_thread.group->mutex);
    } else {
        double time_start_kernel_as_new = wf_timestamp();
        int new_as = wf_kernel_as_new();
        wf_log("- [address-space-new, %.4f, \"%s\"]\n", wf_timestamp() - time_start_kernel_as_new, wf_local_thread.group->name);

        double time_start_kernel_as_switch = wf_timestamp();
        wf_kernel_as_switch(new_as);
        wf_log("- [address-space-switch, %.4f]\n", wf_timestamp() - time_start_kernel_as_switch);
        wf_local_thread.current_as = new_as;

        // Load and Apply the patch
        wf_load_patch();
        
        pthread_mutex_lock(&wf_local_thread.group->mutex);
        wf_local_thread.group->migrated_threads = 0;
        wf_local_thread.group->previous_as = wf_local_thread.group->as;
        wf_local_thread.group->as = new_as;
        ////////////////////////////////////////////////////////////////
        wf_timestamp_start_quiescence = wf_timestamp();
        wf_state = LOCAL_QUIESCENCE;
        wf_local_thread.group->target_generation++;

        // Some applications require a trigger to reach local quiescence
        if (wf_config.trigger_local_quiescence) {
            int trigger_counter = 0;
            while (wf_local_thread.group->migrated_threads < wf_amount_patching_threads()) {
                pthread_mutex_unlock(&wf_local_thread.group->mutex);
                if (wf_config.trigger_local_quiescence(trigger_counter++) == 0) { 
                    pthread_mutex_lock(&wf_local_thread.group->mutex);
                    break;
                }
                msleep(wf_trigger_sleep_ms);
                pthread_mutex_lock(&wf_local_thread.group->mutex);
            }
        }

        while (wf_local_thread.group->migrated_threads < wf_amount_patching_threads())
            pthread_cond_wait(&wf_local_thread.group->cond_from_threads, &wf_local_thread.group->mutex);
        
        if (wf_is_group_quiescence)
            pthread_cond_broadcast(&wf_local_thread.group->cond_to_threads); // Wakeup all sleeping group threads
        
        wf_log("- [finished, %.4f, \"%s\"]\n", wf_timestamp(), wf_local_thread.group->name);
        // After successful migration of all threads, we try to delete
        // the previous address-space generation
        double time_wf_kernel_as_delete = wf_timestamp();
        wf_kernel_as_delete(wf_local_thread.group->previous_as);
        wf_log("- [address-space-delete, %.4f, \"%s\"]\n", wf_timestamp() - time_wf_kernel_as_delete, wf_local_thread.group->name);

        wf_state = IDLE;
        pthread_mutex_unlock(&wf_local_thread.group->mutex);
    }

    log("%s Migration %d in %.4f for Group \"%s\"\n",
        wf_is_global_quiescence ? "Global" : "Local",
        wf_local_thread.group->target_generation,
        wf_timestamp(),
        wf_local_thread.group->name
    );


    // Must be called in all circumstances as thread count could take
    // a lock.
    if (wf_config.patch_done) {
        wf_config.patch_done();
    }

    // Print statistics about VMAs again AFTER every thread has migrated
    count_vmas("after");
}

bool check_if_quiescence() {
    if (wf_state == IDLE)
        return false;
    if (wf_local_thread.current_generation != wf_local_thread.group->target_generation) {
        if (wf_local_thread.state == WF_LOCAL_THREAD_STATE_BIRTH ||
                wf_local_thread.state == WF_LOCAL_THREAD_STATE_ACTIVATED)
            return true;
    }
    return false;
}

bool wf_allow_priority_quiescence();
void wf_global_quiescence(void) {
    if (!check_if_quiescence())
        return;

    // every global quiescence point is also an local quiescence point
    if (!wf_is_global_quiescence) {
        wf_local_quiescence();
        return;
    } 

    if (wf_is_global_quiescence && wf_local_thread.current_generation != wf_local_thread.group->target_generation) {
        pthread_mutex_lock(&wf_local_thread.group->mutex);
        if (!wf_allow_priority_quiescence()) {
          pthread_mutex_unlock(&wf_local_thread.group->mutex);
          return;
        }
        wf_log_reach_quiescence_point();
        wf_local_thread.group->migrated_threads++;
        wf_local_thread.in_global_quiescence = true;
        wf_local_thread.current_generation = wf_local_thread.group->target_generation;
        
        // Scenario: GLOBAL_QUIESCENCE is performed while new threads are spawining.
        // Patch application is performed and worker_thread releases lock. New threads 
        // arrive here and wait for the signal, which will "never" arrive as patch application
        // is already finished.
        if (wf_state == GLOBAL_QUIESCENCE) {
            // Signal that one thread has reached the barrier
            if (wf_config.thread_migrated) {
                int remaining = wf_amount_patching_threads() - wf_local_thread.group->migrated_threads;
                wf_config.thread_migrated(remaining);
            }
            
            if (wf_local_thread.group->migrated_threads == wf_amount_patching_threads()) {
                pthread_cond_signal(&wf_local_thread.group->cond_from_threads);
            }
            // BLOCK: Wait for patcher thread to respond
            pthread_cond_wait(&wf_local_thread.group->cond_to_threads, &wf_local_thread.group->mutex);
            wf_local_thread.in_global_quiescence = false;
        }
        pthread_mutex_unlock(&wf_local_thread.group->mutex);
    }
}

void wf_local_quiescence(void) {
    if (!check_if_quiescence())
        return;
    // Migrate as soon as a new version exists
    if (!wf_is_global_quiescence && wf_local_thread.current_generation != wf_local_thread.group->target_generation) {
        pthread_mutex_lock(&wf_local_thread.group->mutex);
        int as = wf_local_thread.group->as;
        int target_generation = wf_local_thread.group->target_generation;
        wf_local_thread.in_global_quiescence = true;
        pthread_mutex_unlock(&wf_local_thread.group->mutex);

        wf_log_reach_quiescence_point();
        wf_kernel_as_switch(as);
        wf_log_migrated();

        pthread_mutex_lock(&wf_local_thread.group->mutex);
        wf_local_thread.current_generation = target_generation;
        wf_local_thread.current_as = as;

        wf_local_thread.group->migrated_threads++;

        if (wf_config.thread_migrated) {
            int remaining = wf_amount_patching_threads() - wf_local_thread.group->migrated_threads;
            // printf("remaining: %s %d\n", name, remaining);
            wf_config.thread_migrated(remaining);
        }
        if (wf_local_thread.group->migrated_threads == wf_amount_patching_threads()) {
            // last thread; wakeup patcher thread
            pthread_cond_signal(&wf_local_thread.group->cond_from_threads);
        }
        
        if (wf_is_group_quiescence) {
            // Group quiescence is a form of global quiescence, but just for a group.
            // So block the group and wait for all threads to be migrated
            pthread_cond_wait(&wf_local_thread.group->cond_to_threads, &wf_local_thread.group->mutex);
        }
        pthread_mutex_unlock(&wf_local_thread.group->mutex);
    }
}

#define OUTPUT_WF_LOCAL_THREAD_STATE_ACTIVATED NULL
#define OUTPUT_WF_LOCAL_THREAD_STATE_ACTIVATED_FINISHED NULL
#define OUTPUT_WF_LOCAL_THREAD_STATE_DEACTIVATED NULL
#define OUTPUT_WF_LOCAL_THREAD_STATE_DEACTIVATED_FINISHED NULL

//#define OUTPUT_WF_LOCAL_THREAD_INVALID_STATE 1

void wf_log_thread_status(char *message) {
    if (message)
        wf_log("- [%s, %.2f, \"%s\", \"%s\", %d]\n", message, wf_timestamp(), wf_local_thread.name, wf_local_thread.group->name, wf_local_thread.id);
}


void wf_try_signal_thread_amount_changed(int amount_threads_added_removed) {
  if (wf_state != IDLE) { // Patcher thread is perfomring some action
      if (wf_local_thread.current_generation == wf_local_thread.group->target_generation)
          wf_local_thread.group->migrated_threads += amount_threads_added_removed;
      if (wf_local_thread.group->migrated_threads == wf_amount_patching_threads())
          pthread_cond_signal(&wf_local_thread.group->cond_from_threads);
  }
}

/* Start of thread. SHould only be called once, otherwise use wf_thread_activate(). */
void wf_thread_birth_group(char *name, char*group) {
    if (wf_local_thread.state == WF_LOCAL_THREAD_STATE_START) {
        // Special experiment; each thread has its own AS
        if (!wf_is_global_quiescence && wf_config_get("WF_LOCAL_SINGLE_AS", 0) != 0) {
            // Each thread should perform in its own AS
            wf_log_reach_quiescence_point();

            int as_id = wf_kernel_as_new();
            wf_log("- [address-space-new, %.4f, \"%s\", \"%s\"]\n", wf_timestamp(), wf_local_thread.name, wf_local_thread.group->name);

            wf_kernel_as_switch(as_id);
            wf_local_thread.current_as = as_id;
            wf_log_migrated();
            return;
        }
        wf_set_group(group);

        pthread_mutex_lock(&wf_local_thread.group->mutex);

        wf_local_thread.id = thread_unique_id_counter++;
        pthread_mutex_lock(&wf_all_threads_mutex);
        wf_total_threads++;
        wf_all_threads[wf_local_thread.id] = &wf_local_thread;
        pthread_mutex_unlock(&wf_all_threads_mutex);

        wf_local_thread.pthread_id = pthread_self();
        wf_local_thread.name = name;
        wf_local_thread.state = WF_LOCAL_THREAD_STATE_BIRTH;
        
        wf_log_thread_status("birth");

        // Change name of thread for better debugging...
        char new_thread_name[16];
        sprintf(new_thread_name, "%d", wf_local_thread.id);
        pthread_setname_np(pthread_self(), new_thread_name);

        wf_local_thread.group->born_threads++;
        wf_local_thread.group->active_threads++;
        
        wf_try_signal_thread_amount_changed(1);

        pthread_mutex_unlock(&wf_local_thread.group->mutex);

        wf_global_quiescence();
    } else {
#ifdef OUTPUT_WF_LOCAL_THREAD_INVALID_STATE
        wf_log("- [WARNING] Ignoring wf_thread_birth(). Currrent state: %d, Thread: %d\n", wf_local_thread.state, wf_local_thread.id);
#endif
    }
}

/* Start of thread. SHould only be called once, otherwise use wf_thread_activate(). */
void wf_thread_birth(char *name) {
    wf_thread_birth_group(name, NULL);    
}

/* Thread will be considered for patching */
void wf_thread_activate(void) {
    wf_log_thread_status(OUTPUT_WF_LOCAL_THREAD_STATE_ACTIVATED);
    if (wf_local_thread.state == WF_LOCAL_THREAD_STATE_DEACTIVATED) {
        pthread_mutex_lock(&wf_local_thread.group->mutex);
        
        wf_local_thread.state = WF_LOCAL_THREAD_STATE_ACTIVATED;
        wf_local_thread.group->active_threads++;

        if (wf_is_patch_only_active_threads)
          wf_try_signal_thread_amount_changed(1);

        pthread_mutex_unlock(&wf_local_thread.group->mutex);
    } else {
#ifdef OUTPUT_WF_LOCAL_THREAD_INVALID_STATE
        wf_log("- [WARNING] Ignoring wf_thread_activate(). Currrent state: %d, Thread: %d\n", wf_local_thread.state, wf_local_thread.id);
#endif
    }
    wf_log_thread_status(OUTPUT_WF_LOCAL_THREAD_STATE_ACTIVATED_FINISHED);
}

/* End of thread. SHould only be called once, otherwise use wf_thread_deactivate(). */
void wf_thread_death(void) {
    if (wf_local_thread.state == WF_LOCAL_THREAD_STATE_BIRTH ||
            wf_local_thread.state == WF_LOCAL_THREAD_STATE_ACTIVATED ||
            wf_local_thread.state == WF_LOCAL_THREAD_STATE_DEACTIVATED) {

        pthread_mutex_lock(&wf_local_thread.group->mutex);

        pthread_mutex_lock(&wf_all_threads_mutex);
        wf_total_threads--;
        wf_all_threads[wf_local_thread.id] = NULL;
        pthread_mutex_unlock(&wf_all_threads_mutex);

        wf_local_thread.group->born_threads--;
        if (wf_local_thread.state != WF_LOCAL_THREAD_STATE_DEACTIVATED)
            wf_local_thread.group->active_threads--;

        wf_local_thread.state = WF_LOCAL_THREAD_STATE_DEATH;

        wf_try_signal_thread_amount_changed(-1);

        pthread_mutex_unlock(&wf_local_thread.group->mutex);
    } else {
#ifdef OUTPUT_WF_LOCAL_THREAD_INVALID_STATE
        wf_log("- [WARNING] Ignoring wf_thread_death(). Currrent state: %d, Thread: %d\n", wf_local_thread.state, wf_local_thread.id);
#endif
    }
    wf_log_thread_status("death");
}

/* Thread won't be considered for patching anymore */
void wf_thread_deactivate(void) {
    wf_log_thread_status(OUTPUT_WF_LOCAL_THREAD_STATE_DEACTIVATED);
    if (wf_local_thread.state == WF_LOCAL_THREAD_STATE_BIRTH ||
            wf_local_thread.state == WF_LOCAL_THREAD_STATE_ACTIVATED) {

        // wf_global_quiescence(); // Thread is not dead (yet). Check patch.
        pthread_mutex_lock(&wf_local_thread.group->mutex);

        wf_local_thread.group->active_threads--;
        wf_local_thread.state = WF_LOCAL_THREAD_STATE_DEACTIVATED;

        if (wf_is_patch_only_active_threads)
          wf_try_signal_thread_amount_changed(-1);

        pthread_mutex_unlock(&wf_local_thread.group->mutex);
    } else {
#ifdef OUTPUT_WF_LOCAL_THREAD_INVALID_STATE
        wf_log("- [WARNING] Ignoring wf_thread_deactivate(). Currrent state: %d, Thread: %d\n", wf_local_thread.state, wf_local_thread.id);
#endif
    }
    wf_log_thread_status(OUTPUT_WF_LOCAL_THREAD_STATE_DEACTIVATED_FINISHED);
}

int wf_find_lowest_priority(bool in_global_quiescence) {
  for (int priority = 0; priority < wf_amount_priorities; priority++) {
    for (int i = 0; i < wf_total_threads; i++) {
      if (wf_all_threads[i]) {
        if (wf_all_threads[i]->group != wf_local_thread.group)
          continue;

        if (wf_all_threads[i]->in_global_quiescence == in_global_quiescence && wf_all_threads[i]->external_priority == priority) {
          return priority;
        }
      }
    }
  }
  return -1;
}

bool wf_allow_priority_quiescence() {
  if (wf_is_global_quiescence && wf_state != IDLE)
  {
    // Quiescence in progress
    pthread_mutex_lock(&wf_all_threads_mutex);
    int lowest_priority_not_in_quiescence = wf_find_lowest_priority(false);
    int lowest_priority_in_quiescence = wf_find_lowest_priority(true);
    pthread_mutex_unlock(&wf_all_threads_mutex);

    // No priority used/set at all. Quiescence is fine.
    if (lowest_priority_not_in_quiescence == -1 && lowest_priority_in_quiescence == -1)
      return true;

    // Higher or equal priorities are already in quiescence, so its fine to also quiescence.
    if (lowest_priority_in_quiescence >= wf_local_thread.external_priority)
      return true;

    // Only higher priorities remain (not in quiescence). All other threads have at least same priority,
    // so it is fine to quiescence.
    if (lowest_priority_not_in_quiescence >= wf_local_thread.external_priority)
      return true;
    
    if (wf_log_quiescence_priority) {
        wf_log("- [no-quiescence-priority, %.4f, \"%s\", \"%s\", %d, Own: %d, Lowest Q: %d, Lowest !Q: %d]\n",
               wf_timestamp(), wf_local_thread.name, wf_local_thread.group->name, wf_local_thread.id,
               wf_local_thread.external_priority, lowest_priority_in_quiescence, lowest_priority_not_in_quiescence);
    }

    return false;
  }
}

void wf_thread_set_priority(int priority) {
  pthread_mutex_lock(&wf_local_thread.group->mutex);
  wf_local_thread.external_priority = priority;
  pthread_mutex_unlock(&wf_local_thread.group->mutex);
}

void wf_init(struct wf_configuration config) {
    wf_amount_priorities = config.amount_priorities;
    wf_trigger_sleep_ms = wf_config_get("WF_TRIGGER_SLEEP_MS", config.trigger_sleep_ms);
    wf_log_quiescence_priority = wf_config_get("WF_LOG_QUIESCENCE_PRIORITY", wf_log_quiescence_priority);
    
    wf_every_action_delay_ms = wf_config_get("WF_EVERY_ACTION_DELAY_MS", 0);
    latencies = calloc(1000, sizeof(struct patch_measurement));

    int wf_global = wf_config_get("WF_GLOBAL", 1);
    wf_is_global_quiescence = wf_global == 0 ? false : true;
   
    int wf_patch_only_active = wf_config_get("WF_PATCH_ONLY_ACTIVE", 0);
    wf_is_patch_only_active_threads = wf_patch_only_active == 0 ? false : true;

    char *group_quiescence = getenv("WF_GROUP");
    if (group_quiescence == NULL) {
        wf_is_group_quiescence = false;
    } else {
        wf_is_group_quiescence = true;
        wf_group_quiescence = group_quiescence;
    }

    if (wf_is_global_quiescence && wf_is_group_quiescence) {
        fprintf(stderr, "global quiescence is not possible with group quiescence!\n");
        exit(1);
    }

    wf_load_symbols("/proc/self/exe");

    pthread_condattr_t attr;
    pthread_condattr_init(&attr);
    pthread_condattr_setclock(&attr, CLOCK_REALTIME);
    pthread_cond_init(&wf_cond_initiate, &attr);

    // Copy(!) away the configuration that we got from the
    // configuration
    wf_config = config;

    char *logfile = getenv("WF_LOGFILE");
    if (logfile) {
        log("opening wf logfile: %s\n", logfile);
        wf_log_file = fopen(logfile, "w+");
    } else {
        wf_log_file = stderr;
    }

    // Load the patch queue, if possible
    char *queue = getenv("WF_PATCH_QUEUE");
    if (queue) wf_patch_queue = strdup(queue);

    // We start a thread that does all the heavy lifting of address
    // space management
    if ((errno = pthread_create(&wf_patch_thread, NULL,
                                &wf_patch_thread_entry, NULL)) != 0) {
        perror("pthread_create");
    }

    pthread_setname_np(wf_patch_thread, "wf-patcher");

    count_vmas("startup");
}

// ############################################ //
// ############### VMA COUNT ################## //
// ############################################ //

typedef struct {
    long shared_clean;
    long shared_dirty;
    long private_clean;
    long private_dirty;
    long size;

    int count;

    char *perms;

    int is_anon;
    int is_file_backed;
} vma_count;

static int count_vmas_amount_perms = 16;

static void count_vmas_init_vma_count(vma_count vma_count_list[]) {
    char *perms[] = {
        "---p", "---s",
        "r--p", "r--s",
        "-w-p", "-w-s",
        "--xp", "--xs",
        "rw-p", "rw-s",
        "r-xp", "r-xs",
        "-wxp", "-wxs",
        "rwxp", "rwxs"
    };

    for (int i = 0; i < count_vmas_amount_perms; i++) {
        memset(&vma_count_list[i], 0, sizeof(vma_count)); // All variables to 0
        vma_count_list[i].is_anon = 1;
        vma_count_list[i].perms = perms[i];
        
        memset(&vma_count_list[i + count_vmas_amount_perms], 0, sizeof(vma_count)); // All variables to 0
        vma_count_list[i + count_vmas_amount_perms].is_file_backed = 1;
        vma_count_list[i + count_vmas_amount_perms].perms = perms[i];
    }
       
}

static vma_count *count_vmas_find_vma_count(vma_count vma_count_list[], char *perms, char *inode) {
    int start;
    int end;
    if (strcmp(inode, "0") == 0) {
        // Anonymous VMA
        start = 0;
    } else {
        // File-Backed VMA
        start = count_vmas_amount_perms;
    }
    end = start + count_vmas_amount_perms;

    for (int i = start; i < end; i++) {
        if (strcmp(vma_count_list[i].perms, perms) == 0)
            return &vma_count_list[i];
    }
    return NULL;
}

/* Prints statistics about the VMA of the current process
 * (/proc/self/smaps).
 *
 * It sums up the:
 * Size, Shared Clean, Shared Dirty, Private Clean, Private Dirty
 * memory for each group. A grouop consists of the tuple:
 * permission (perms) and anonymous/file-backed VMA
 *
 * It also counts the number of VMAs for each group. 
 *
 * Only the groups where count > 0 are printed.
 *
 * Output format:
 * -[vma-count, <WFPATCH-GENERATION>, <PERM>, (ANONYMOUS|FILE-BACKED), <COUNT>, \
 *  <sum SIZE>, <sum SHARED CLEAN>, <sum SHARED DIRTY>, <sum PRIVATE CLEAN>, <sum PRIVATE DIRTY>]
 * */
static void count_vmas(char *output_suffix) {
    const char* s = getenv("WF_MEASURE_VMA"); 
    if (!s) return;

    if (atoi(s) > 0) {
        // Check VMA count
        FILE *fp;
        char *line = NULL;
        size_t len = 0;
        ssize_t read;

        fp = fopen("/proc/self/smaps", "r");
        if (fp == NULL) return;

        vma_count vma_count_list[count_vmas_amount_perms * 2];
        count_vmas_init_vma_count(vma_count_list);

        char *const_size = "Size:";
        char *const_shared_clean = "Shared_Clean:";
        char *const_shared_dirty = "Shared_Dirty:";
        char *const_private_clean = "Private_Clean:";
        char *const_private_dirty = "Private_Dirty:";


        regex_t header_regex;
        int header_regex_result;
        header_regex_result = regcomp(&header_regex, "^([0-9a-f]+-[0-9a-f]+) (....) ([0-9a-f]+) (..):(..) .*", REG_EXTENDED);
        if (header_regex_result) {
            fprintf(stderr, "Could not compile regex");
            exit(1);
        }

        vma_count *current_state = NULL;
        while ((read = getline(&line, &len, fp)) != -1) {
            header_regex_result = regexec(&header_regex, line, 0, NULL, 0);
            if (!header_regex_result) {
                strtok(line, " ");
                char *perms = strtok(NULL, " "); // Permission
                strtok(NULL, " "); // Offset
                strtok(NULL, " "); // Time
                char *inode = strtok(NULL, " "); // inode

                current_state = count_vmas_find_vma_count(vma_count_list, perms, inode);
                current_state->count += 1;
            } else if (header_regex_result == REG_NOMATCH && current_state) {
                strtok(line, " ");
                if (strncmp(const_size, line, strlen(const_size)) == 0) {
                    char *size = strtok(NULL, " ");
                    int vma_size = atoi(size);
                    current_state->size += vma_size;
                } else if (strncmp(const_shared_clean, line, strlen(const_shared_clean)) == 0) {
                    char *size = strtok(NULL, " ");
                    int shared_clean = atoi(size);
                    current_state->shared_clean += shared_clean; 
                } else if (strncmp(const_shared_dirty, line, strlen(const_shared_dirty)) == 0) {
                    char *size = strtok(NULL, " ");
                    int shared_dirty = atoi(size);
                    current_state->shared_dirty += shared_dirty;
                } else if (strncmp(const_private_clean, line, strlen(const_private_clean)) == 0) {
                    char *size = strtok(NULL, " ");
                    int private_clean = atoi(size);
                    current_state->private_clean += private_clean;
                } else if (strncmp(const_private_dirty, line, strlen(const_private_dirty)) == 0) {
                    char *size = strtok(NULL, " ");
                    int private_dirty = atoi(size);
                    current_state->private_dirty += private_dirty;
                }
            } else {
                char msgbuf[100];
                regerror(header_regex_result, &header_regex, msgbuf, sizeof(msgbuf));
                fprintf(stderr, "Regex match failed: %s\n", msgbuf);
                exit(1);
            }
        }

        free(line);
        fclose(fp);

        for (int i = 0; i < count_vmas_amount_perms * 2; i++) {
            vma_count ele = vma_count_list[i];

            if (ele.count == 0) continue;
            
            wf_log("- [vma-count-%s, %d, %s, %s, %d, %d, %d, %d, %d, %d]\n",
                    output_suffix, wf_local_thread.group == NULL ? 0: wf_local_thread.group->target_generation,
                    ele.perms, ele.is_anon > 0 ? "ANONYMOUS": "FILE-BACKED", ele.count, ele.size, ele.shared_clean, ele.shared_dirty, ele.private_clean, ele.private_dirty);
        }
    }
}

char *trim(char *str) {
    char *ptr = NULL;
    // Start
    while (*str == ' ' || *str == '\t' || *str == '\n') str++;
    ptr = str + strlen(str) - 1;
    // End
    while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n'){ *ptr = '\0' ; ptr--; } ;
    return str;   
}

int page_table_size(int iteration) {
    const char* s = getenv("WF_MEASURE_PTE"); 
    if (!s) return;

    FILE *fp = fopen("/proc/self/status", "r");
    ssize_t read;
    char *line = NULL;
    size_t len = 0;
    while ((read = getline(&line, &len, fp)) != -1 ) {
        if (strncmp("VmPTE:", line, strlen("VmPTE:")) == 0) {
            line = line + strlen("VmPTE:");
            line = trim(line);
            line[strlen(line) - strlen("kB")] = '\0';
            line = trim(line);
            wf_log("[VmPTE %d, %s]\n", iteration, line);
            return atoi(line);
        }
    }
}

