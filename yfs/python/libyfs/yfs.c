#include <Python.h>

#define DBG_SUBSYS S_LIBYLIB
#include "sdfs_lib.h"
#include "md_lib.h"
#include "net_global.h"
#include "dbg.h"

static PyObject *YFSError;

/* yfs_lib.c */
static PyObject *
yinit(PyObject *self, PyObject *args)
{
        int ret;
        ret = ly_init(0, NULL);

        return Py_BuildValue("i", ret);
}

PyDoc_STRVAR(yinit_doc,
"init(None) -> return success of failed"
);

static PyObject *
ydestroy(PyObject *self, PyObject *args)
{
        int ret;
        ret = ly_destroy();

        return Py_BuildValue("i", ret);
}

/* node.c */
static PyObject *
ymkdir(PyObject *self, PyObject *args)
{
        int ret;
        const char *path;
        const int mode;

        if (!PyArg_ParseTuple(args, "si", &path, &mode))
                return NULL;

        ret = ly_mkdir(path, (mode_t) mode);

        return Py_BuildValue("i", ret);
}

static PyObject *
yrmdir(PyObject *self, PyObject *args)
{
        int ret;
        const char *path;

        if (!PyArg_ParseTuple(args, "s", &path))
                return NULL;

        ret = ly_rmdir(path);

        return Py_BuildValue("i", ret);
}

static PyObject *
yunlink(PyObject *self, PyObject *args)
{
        int ret;
        const char *path;

        if (!PyArg_ParseTuple(args, "s", &path))
                return NULL;

        ret = ly_unlink(path);

        return Py_BuildValue("i", ret);
}


static PyObject *
yopen(PyObject *self, PyObject *args)
{
        int ret;
        const char *path;

        if (!PyArg_ParseTuple(args, "s", &path))
                return NULL;

        ret = ly_open(path);

        return Py_BuildValue("i", ret);
}


static PyObject *
yrelease(PyObject *self, PyObject *args)
{
        int ret;
        const int fd;

        if (!PyArg_ParseTuple(args, "i", &fd))
                return NULL;

        ret = ly_release(fd);

        return Py_BuildValue("i", ret);
}

static PyObject *
ycreate(PyObject *self, PyObject *args)
{
        int ret;
        const char *path;
        const int mode;

        if (!PyArg_ParseTuple(args, "si", &path, &mode))
                return NULL;

        ret = ly_create(path, (mode_t) mode);

        return Py_BuildValue("i", ret);
}

static PyObject *
ypread(PyObject *self, PyObject *args)
{
        size_t maxsize;
        const uint64_t off;
        const int fd;
        int read_size;
        char  readbuf[524288];

        if (!PyArg_ParseTuple(args, "iiK", &fd, &maxsize, &off))
                return NULL;

        if (maxsize > 524288)
                maxsize = 524288;

        read_size = ly_pread(fd, readbuf, maxsize, off);

        return Py_BuildValue("(is)", read_size, readbuf);
}

static PyObject *
ypwrite(PyObject *self, PyObject *args)
{
        const size_t buflen;
        const uint64_t off;
        const int fd;
        const char *buf;
        int writelen;

        if (!PyArg_ParseTuple(args, "isiK", &fd, &buf, &buflen, &off))
                return NULL;

        writelen = ly_pwrite(fd, buf, buflen, off);

        return Py_BuildValue("i", writelen);
}

static PyObject *
ymdc_badchk(PyObject *self, PyObject *args)
{
        int ret;
        const uint64_t chk_id;
        const uint32_t chk_version;
        const uint64_t disk_id;
        const uint32_t disk_version;

        if (!PyArg_ParseTuple(args, "KIKI", &chk_id, &chk_version,
                                &disk_id, &disk_version))
                return NULL;

        ret = md_badchk(chk_id, chk_version,
                        disk_id, disk_version);

        return Py_BuildValue("i", ret);
}

static struct PyMethodDef yfs_methods[] = {
        {"init", yinit, METH_VARARGS,
                "init the yfs connection."},

        {"destroy", ydestroy, METH_VARARGS,
                "disconnect to yfs."},

        {"mkdir", ymkdir, METH_VARARGS,
                "mkdir in yfs"},

        {"rmdir", yrmdir, METH_VARARGS,
                "remove a dir from yfs"},

        {"unlink", yunlink, METH_VARARGS,
                "unlink a file from yfs."},

        {"open", yopen, METH_VARARGS,
                "open a file."},

        {"release", yrelease, METH_VARARGS,
                "close a open file fd."},

        {"create", ycreate, METH_VARARGS,
                "create a empty file in yfs."},

        {"pread", ypread, METH_VARARGS,
                "read maxsize byte from fd."},

        {"pwrite", ypwrite, METH_VARARGS,
                "write buf to fd."},

        {"mdc_badchk", ymdc_badchk, METH_VARARGS,
                "report bad chunk to mds."},

        {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC
inityfs(void)
{
        PyObject *m;
        m = Py_InitModule("yfs", yfs_methods);
        if (m == NULL)
                return;

        YFSError = PyErr_NewException("yfs.error", NULL, NULL);
        Py_INCREF(YFSError);
        PyModule_AddObject(m, "error", YFSError);
}

