OBJDIRS += backend

# Create template specific variables
UPPERC_DIR := BACKEND
LOWERC_DIR := backend

BACKEND_SRCS := backend/simple_storage.cc


SRC_LINKED_OBJECTS := $(PROTO_OBJS)
TEST_LINKED_OBJECTS := $(PROTO_OBJS) $(COMMON_OBJS) $(CHECKPOINT_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)
	
# Need to specify test cases explicitly because they have variables in recipe
test-backend: $(BACKEND_TESTS)
	@echo + Ensuring Database Storage Layer Exists...
	@mkdir -p $(STORE)
	@echo + Ensuring Checkpoint Storage Layer Exists...
	@mkdir -p $(CHKPNT)
	@for a in $(BACKEND_TESTS); do \
		echo == $$a ==; \
		$(LDLIBRARYPATH) $$a; \
	done
