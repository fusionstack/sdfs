#noinst_PROGRAMS = \
#	ychktable_print

bin_PROGRAMS = \
	ycp \
	ychkstat \
	ychmod \
	yls \
	ymkdir \
	yrm \
	yrmdir \
	ystat \
	ystatvfs \
	ylvm \
	yping \
	uss.du \
	uss.ns \
	uss.ls \
	uss.mkdir \
	uss.rmdir \
	uss.mv \
	uss.rm \
	uss.stat \
	uss.touch \
	uss.truncate \
	uss.mdstat \
	uss.cdstat \
	uss.mdsdrop \
	uss.objck \
	uss.objmv \
	uss.tgtadm \
	uss.clone \
	uss.md5sum \
	uss.sysop \
	uss.chkstat \
	uss.ffc \
	uss.attr \
	uss.hsm \
	uss.fallocate \
	uss.license\
	uss.worm

#snap_rollback_SOURCES=snap_rollback.c

uss_fallocate_SOURCES=uss.fallocate.c

uss_attr_SOURCES=yattr.c

yping_SOURCES=yping.c

#cdsop_read_SOURCES=cdsop_read.c

ylvm_SOURCES=ylvm.c

uss_du_SOURCES=uss.du.c

uss_ns_SOURCES=uss.ns.c

uss_ls_SOURCES=uss.ls.c
uss_chkstat_SOURCES=uss.chkstat.c

uss_mkdir_SOURCES=uss.mkdir.c

uss_rmdir_SOURCES=uss.rmdir.c

uss_mv_SOURCES=uss.mv.c

uss_rm_SOURCES=uss.rm.c

uss_stat_SOURCES=uss.stat.c

uss_touch_SOURCES=uss.touch.c

uss_truncate_SOURCES=uss.truncate.c
uss_mdstat_SOURCES=uss.mdstat.c

uss_mdsdrop_SOURCES=uss.mdsdrop.c

uss_cdstat_SOURCES=uss.cdstat.c

uss_objck_SOURCES = uss.objck.c

uss_objmv_SOURCES = uss.objmv.c

uss_tgtadm_SOURCES=uss.tgtadm.c

uss_clone_SOURCES=uss.clone.c

uss_md5sum_SOURCES=uss.md5sum.c

uss_sysop_SOURCES=uss.sysop.c

uss_ffc_SOURCES=ffc.c

uss_worm_SOURCES=uss.worm.c

#ychkcp_SOURCES=ychkcp.c

#cmd_SOURCES=cmd.c

#ysetopt_SOURCES=ysetopt.c

#yrecover_SOURCES = yrecover.c

ychkstat_SOURCES = \
	ychkstat.c

#jnlfind_SOURCES = jnlfind.c

ychmod_SOURCES = \
	ychmod.c

#ylogin_SOURCES = ylogin.c

yls_SOURCES = \
	yls.c

ymkdir_SOURCES = \
	ymkdir.c

#yrep_SOURCES = yrep.c

yrmdir_SOURCES = \
	yrmdir.c

yrm_SOURCES = \
	yrm.c

ystat_SOURCES = \
	ystat.c

ystatvfs_SOURCES = \
	ystatvfs.c

uss_hsm_SOURCES=uss.hsm.c
uss_license_SOURCES=uss.license.c

#yshutdown_SOURCES =  yshutdown.c

#ychunk_check_SOURCES= ychunk_check.c

#ytruncate_SOURCES = ytruncate.c

ycp_SOURCES = \
	ycp.c

AM_LDFLAGS = @LDFLAGS@ \
	-L../../parser/lib -lparser \
	-L../../ylib/lib -lylib \
	-L../build -lyfs \
	-L../../ynet/build -lynet \
	-lpthread -lcrypt -lcrypto -luuid -laio -lssl
