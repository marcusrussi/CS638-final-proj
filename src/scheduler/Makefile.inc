OBJDIRS += scheduler

# Create template specific variables
UPPERC_DIR := SCHEDULER
LOWERC_DIR := scheduler

SCHEDULER_PROG :=
SCHEDULER_SRCS := scheduler/partitioned_transaction_manager.cc \
                  scheduler/traditional_lock_manager.cc \
                  scheduler/traditional_executor.cc \
                  scheduler/traditional_deadlock_executor.cc \
                  scheduler/waitforgraph_lock_manager.cc \
                  scheduler/waitdie_lock_manager.cc \
                  scheduler/dreadlock_lock_manager.cc \
                  scheduler/localwaitforgraph_lock_manager.cc \
                  scheduler/traditional_transaction_manager.cc \
                  scheduler/partitioned_lock_manager.cc \
                  scheduler/partitioned_executor.cc \
                  scheduler/hstore_transaction_manager.cc \
                  scheduler/hstore_executor.cc \
                  scheduler/physical_partitioned_lock_manager.cc \
                  scheduler/physical_partitioned_executor.cc \
                  scheduler/physical_partitioned_transaction_manager.cc \
                  scheduler/physical_traditional_lock_manager.cc \
                  scheduler/physical_traditional_executor.cc


SRC_LINKED_OBJECTS :=
TEST_LINKED_OBJECTS := $(PROTO_OBJS) $(COMMON_OBJS) $(BACKEND_OBJS) \
                       $(APPLICATION_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

# Need to specify test cases explicitly because they have variables in recipe
test-scheduler: $(SCHEDULER_TESTS)
	@for a in $(SCHEDULER_TESTS); do \
		echo == $$a ==; \
		$(LDLIBRARYPATH) $$a; \
	done
