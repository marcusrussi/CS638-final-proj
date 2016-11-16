#
# This makefile system follows the structuring conventions
# recommended by Peter Miller in his excellent paper:
#
#       Recursive Make Considered Harmful
#       http://aegis.sourceforge.net/auug97.pdf
#

# We create a listing of the root directories for access into
OBJDIR := ../obj
BINDIR := ../bin
EXTDIR := ../ext
SRCDIR := ../src
DBDIR  := ../db
STORE  := ../db/storage


# '$(V)' controls whether the lab makefiles print verbose commands (the
# actual shell commands run by Make), as well as the "overview" commands
# (such as '+ cc lib/readline.c').
#
# For overview commands only, the line should read 'V = @'.
# For overview and verbose commands, the line should read 'V ='.
V = @

# Set the compiler and compile-time loaded libraries
CXX           := g++
# For profiling (with gprof), this line should read 'PG = -pg'
# For fast execution, this line should read 'PG ='.
PG = 

# Set the flags for C++ to compile with (namely where to look for external
# libraries) and the linker libraries (again to look in the ext/ library)
CXXFLAGS := -g -O2 -MD $(PG) -I$(SRCDIR) -I$(OBJDIR)
CXXFLAGS += -Wall -Werror --std=c++0x

LDFLAGS := -lrt -lpthread $(PG) \
           -lnuma
#           -lprofiler

# Lists that the */Makefile.inc makefile fragments will add to
OBJDIRS :=
TESTS :=

# Make sure that 'all' is the first target
all:
	@echo + Ensuring Database Storage Layer Exists...
	@mkdir -p $(STORE)
	
# Makefile template so that the makefrags are far less redundant
MAKEFILE_TEMPLATE := Makefile.template

# Makefile fragments for components
include applications/Makefile.inc
include scheduler/Makefile.inc
include common/Makefile.inc
include backend/Makefile.inc

# Makefile fragments for executable and deployment scripts
include deployment/Makefile.inc

test: $(TESTS)

clean:
	rm -rf $(OBJDIR) $(BINDIR) $(DBDIR) $(STORE)

# This magic automatically generates makefile dependencies
# for header files included from C source files we compile,
# and keeps those dependencies up-to-date every time we recompile.
# See 'mergedep.pl' for more information.
$(OBJDIR)/.deps: $(foreach dir, $(OBJDIRS), $(wildcard $(OBJDIR)/$(dir)/*.d))
	@mkdir -p $(@D)
	@cat $^ /dev/null > $@
#	@$(PERL) mergedep.pl $@ $^
-include $(OBJDIR)/.deps

always:

# Eliminate default suffix rules
.SUFFIXES:

# make it so that no intermediate .o files are ever deleted
.PRECIOUS: $(foreach dir, $(OBJDIRS), $(OBJDIR)/$(dir)/%.o) \
           $(foreach dir, $(OBJDIRS), $(OBJDIR)/$(dir)/%.pb.cc) \
           $(foreach dir, $(OBJDIRS), $(OBJDIR)/$(dir)/%.pb.h)

.PHONY: all always clean test
