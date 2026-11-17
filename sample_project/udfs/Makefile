# Rebuilds the example UDF packages from their sources (see README.md).
#
#   build/tshirt_sizing.zip  the Python UDF in tshirt_sizing/
#   build/shout.jar          the Java UDF in shout/ (needs Maven and a JDK)
#
# Everything the Makefile produces ends up in build/ (except Maven's own shout/target/), so
# that folder can be git-ignored. Run `make` here. The zip is not deterministic (the ZIP entry carries the build time), so
# rebuilding changes its hash.

SOURCES := $(shell find tshirt_sizing -type f -not -path '*/__pycache__/*' -not -path '*/dist/*')
BUILD := build

# Wraps the sdist tarball in a ZIP, as Confluent requires. The entry keeps the tarball's
# modification time, so rebuilding unchanged sources gives different bytes each time.
define ZIP_SDIST
python3 -c "import sys,zipfile,glob,os; \
 s=glob.glob(sys.argv[1]+'/*.tar.gz')[0]; \
zipfile.ZipFile(sys.argv[2],'w').write(s, os.path.basename(s))" $(1) $(2)
endef

.PHONY: all clean
all: $(BUILD)/tshirt_sizing.zip $(BUILD)/shout.jar

$(BUILD)/tshirt_sizing.zip: $(SOURCES)
	rm -rf $(BUILD)/tshirt_sizing-sdist && mkdir -p $(BUILD)
	uv build --sdist --directory tshirt_sizing --out-dir $(abspath $(BUILD))/tshirt_sizing-sdist
	$(call ZIP_SDIST,$(BUILD)/tshirt_sizing-sdist,$@)

JAVA_SOURCES := $(shell find shout -type f -not -path 'shout/target/*')

$(BUILD)/shout.jar: $(JAVA_SOURCES)
	mvn -q -f shout/pom.xml clean package
	mkdir -p $(BUILD)
	cp shout/target/shout.jar $@

clean:
	rm -rf $(BUILD) shout/target
