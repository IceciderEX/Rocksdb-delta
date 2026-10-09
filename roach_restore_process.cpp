/*
 * src/agent/roach_restore_process.cpp
 */
#include "roach_restore_process.h"

#include "restore/restore_main.h"
#include "roach_read_only.h"
#include "roach_restore_common.h"
#include "roach_process.h"
#include "agent_extern.h"
#include "restore.h"
#include "restore/restore_clean.h"
#include "file_list.h"
#include "delay_restore_file.h"
#include "roach_agent.h"
#include "storage_manager.h"
#include "roach_common.h"
#include "agent_metadata.h"
#include "securec_check.h"

TaskOrderMsg g_taskOrderMsgList[] =
    {
        {BEFORE_BEGIN_RESTORE, "BEFORE_BEGIN_RESTORE"},
        {BEFORE_BEGIN_RESTORE_READ_ONLY, "BEFORE_BEGIN_RESTORE_READ_ONLY"},
        {BEFORE_UPDATE_SNAPSHOT_READ_ONLY, "BEFORE_UPDATE_SNAPSHOT_READ_ONLY"},
        {AFTER_UPDATE_SNAPSHOT_READ_ONLY, "AFTER_UPDATE_SNAPSHOT_READ_ONLY"}
    };

const char* g_nonDataDirs[] = {"pg_clog", "pg_errorinfo", "pg_multixact", "pg_notify", "pg_residualfiles",
                               "pg_replslot", "pg_serial", "pg_snapshots", "pg_stat_tmp",
                               "pg_subtrans", "pg_twophase", "pg_xlog", "pg_cbm", NULL};
const char* g_NonDataDirsForReadOnly[] = {"pg_twophase", NULL};
const char* g_excludeSuffixPatternList[] = {".history", NULL};
const char* g_cnExcludeFileList[] = {"postgresql.conf", "postgresql.conf.new", "pg_hba.conf", "pg_hba.conf.new",
                                     "pg_ident.conf", "pg_ident.conf.new", "roachbackup", "kvcache.conf", "kvcache.conf.new", "kv.conf", "kv.conf.new", NULL};

ERROR_CODE PerformDoTasksBeforeRestore(int procSlotId, TaskOrder taskOrder);

ERROR_CODE PerformTasksBeforeRestore(int procSlotId)
{
   ERROR_CODE ec = EC_SUCCESS;

   if (globals->operation != RESTORE) {
       return ec;
   }

   if (clioptions.bIncremental == false) {
       return ec;
   }

   /* reach here only increment restore mode. */
   if (clioptions.readOnlyStandbyCluster) {
       /* read only standby cluster mode. */
       ec = PerformDoTasksBeforeRestore(procSlotId, BEFORE_BEGIN_RESTORE_READ_ONLY);
   } else {
       /* off line restore mode. */
       ec = PerformDoTasksBeforeRestore(procSlotId, BEFORE_BEGIN_RESTORE);
   }
   return ec;
}

/* do some tasks before restore. */
ERROR_CODE PerformDoTasksBeforeRestore(int procSlotId, TaskOrder taskOrder)
{
    ERROR_CODE ec = EC_SUCCESS;

    LOGINFO("Perform tasks in state:%s.", g_taskOrderMsgList[taskOrder].taskOrderMsg);

    ec = HandleDNTasks(procSlotId, taskOrder);
    if (ec != EC_SUCCESS) {
        return ec;
    }

    ec = HandleCNTasks(procSlotId, taskOrder);
    if (ec != EC_SUCCESS) {
        return ec;
    }

    ec = HandleStandbyDNTasks(procSlotId, taskOrder);
    if (ec != EC_SUCCESS) {
        return ec;
    }

    LOGINFO("Finish tasks in state:%s.", g_taskOrderMsgList[taskOrder].taskOrderMsg);
    return ec;
}

ERROR_CODE DoCNTasksBeforeRestore(COORD_METADATA *coord, TaskOrder TaskOrder)
{
    ERROR_CODE ec = EC_SUCCESS;
    LOGINFO("clean CN directory %s before %s restore.", coord->localDataPath,
            coord->backupType == BACKUP_MODE_FULL ? "Full" : "Inc");

    if (coord->backupType == BACKUP_MODE_FULL) {
        ec = removeDirectory(coord->localDataPath, false, g_cnExcludeFileList, NULL);
        if (ec != EC_SUCCESS) {
            LOGINFO("Warning: Failed to remove cn stale directory %s before incremental restore", coord->localDataPath);
        }
    } else {
        if (TaskOrder == BEFORE_BEGIN_RESTORE) {
            /* off line mode. */
            ec = IncRestoreRemoveDirectory(coord->localDataPath, g_nonDataDirs, g_excludeSuffixPatternList);
        } else {
            /* read only mode. */
            ec = IncRestoreRemoveDirectory(coord->localDataPath, g_NonDataDirsForReadOnly, g_excludeSuffixPatternList);
        }
        if (ec != EC_SUCCESS) {
            LOGERROR("Failed to remove cn stale directory before incremental restore.");
        }
    }
    return ec;
}

ERROR_CODE CheckMedadataCount(NODE_METADATA *meta)
{
    if (clioptions.remoteMasterIp == NULL) {
        if (meta->datanodeCount != agentGlobals->nodeMetadata->datanodeCount) {
            LOGERROR("Cannot restore new cluster when data node count cannot match.");
            return EC_INVALID_COMMAND;
        }

        if (meta->standbyCount != agentGlobals->nodeMetadata->standbyCount) {
            LOGERROR("Cannot restore new cluster when standby node count cannot match.");
            return EC_INVALID_COMMAND;
        }

        if (meta->dummyStandbyCount != agentGlobals->nodeMetadata->dummyStandbyCount) {
            LOGERROR("Cannot restore new cluster when dummy standby node count cannot match.");
            return EC_INVALID_COMMAND;
        }
    }

    return EC_SUCCESS;
}

ERROR_CODE CheckAndInitializeBfRestore(NODE_METADATA *meta, THREAD_CONTEXT *threadCtx)
{
    ERROR_CODE ulRet = EC_SUCCESS;

    /* check if old content exist */
    if (clioptions.bIncremental == false) {
        ulRet = checkContentExists(meta);
        if (ulRet != EC_SUCCESS) {
            LOGERROR("Content exists when perform restore, msg: %s", getErrmsg(ulRet));
            return ulRet;
        }
    }

    ulRet = initializeParallelTasks(false, threadCtx);
    if (ulRet != EC_SUCCESS) {
        LOGINFO("Failed to init parallel task, msg: %s", getErrmsg(ulRet));
        cleanChildProcess();
        return ulRet;
    }

    return ulRet;
}

ERROR_CODE RemoveFilesAfterRestore(NODE_METADATA *meta)
{
    ERROR_CODE ulRet = EC_SUCCESS;
    size_t idx = 0;

    /* remove cm server directry if there is no cm server in current node */
    ulRet = RemoveCms();
    if (ulRet != EC_SUCCESS) {
        LOGINFO("Failed to remove cms dir, msg: %s", getErrmsg(ulRet));
        return ulRet;
    }

    /* Remove the roach restore file in instance */
    if (clioptions.bIncremental == false) {
        ulRet = removeNodeRoachRestoreFile(meta);
        if (ulRet != EC_SUCCESS) {
            LOGINFO("Failed to remove roach.restore files, msg: %s", getErrmsg(ulRet));
            return ulRet;
        }
    } else {
        char *instanceName = NULL;

        /*
         * This needs special handling after support from kernel team, currently stale files can be left as it is for
         * incremental backup. if it is incremental restore, Remove all stale files came from full backup(if it exist)
         */
        for (idx = 0; idx < meta->datanodeCount; idx++) {
            ulRet = getInstanceName(meta->datanodes[idx].localDataPath, &instanceName);
            if (ulRet != EC_SUCCESS) {
                FREE(instanceName);
                LOGERROR("Failed to get InstanceName for the path %s ", meta->datanodes[idx].localDataPath);
                return ulRet;
            }
            (void)removeStaleFiles(instanceName, meta->datanodes[idx].localDataPath, true);
            (void)removeStaleFiles(instanceName, meta->datanodes[idx].localDataPath, false);
            FREE(instanceName);
            instanceName = NULL;
        }
        /* CN instance is full restore, no need to check deleted files */
    }

    return ulRet;
}

ERROR_CODE PerformProcRestore(THREAD_CONTEXT *threadCtx)
{
    ERROR_CODE ulRet = EC_SUCCESS;

    /* Restore the db files */
    ulRet = performRestore(PARENT_PROC_SLOTID);
    if (ulRet == EC_SUCCESS) {
        ulRet = waitForDiskWriterThread(XBSA_THREAD_STATE_FINISHED);
    } else {
        LOGINFO("Failed to perform restore in parent, ret: %s", getErrmsg(ulRet));
        (void)waitForDiskWriterThread(XBSA_THREAD_STATE_ERROR);
        setProcessState(PARENT_PROC_SLOTID, PROC_FAILURE);
        return ulRet;
    }

    setProcessState(PARENT_PROC_SLOTID, PROC_DATACOPYING_DONE);

    /* Wait until all of them finish copying data files */
    ulRet = waitForAllProcState(PROC_DATACOPYING_DONE, threadCtx);
    if (ulRet != EC_SUCCESS) {
        return ulRet;
    }

    return ulRet;
}

STATIC ERROR_CODE PerformRestoreTasks(THREAD_CONTEXT *threadCtx, NODE_METADATA *meta)
{
    ERROR_CODE ulRet = EC_SUCCESS;

    StorageConfig mediaConfigInfo;
    ulRet = InitStorageConfig(&mediaConfigInfo, agentGlobals->nodeMetadata->nodeHostName);
    if (ulRet != EC_SUCCESS) {
        LOGERROR("Failed to malloc storage config!");
        return ulRet;
    }

    agentGlobals->smgr = media::CreateStorageManager(clioptions.mediaType, &mediaConfigInfo);
    if (agentGlobals->smgr == NULL) {
        LOGERROR("Failed to init smgr object when perform restore parallely.");
        LOGCONTENTS("Backup Sender thread exited");
        setProcessState(PARENT_PROC_SLOTID, PROC_FAILURE);
        return EC_SMGR_FAILED;
    }

    ulRet = DestroyStorageConfig(&mediaConfigInfo, agentGlobals->nodeMetadata->nodeHostName);
    if (ulRet != EC_SUCCESS) {
        return ulRet;
    }

    ulRet = CheckAndInitializeBfRestore(meta, threadCtx);
    if (ulRet != EC_SUCCESS) {
        return ulRet;
    }

    ulRet = PerformProcRestore(threadCtx);
    if (ulRet != EC_SUCCESS) {
        return ulRet;
    }

    if (!clioptions.readOnlyStandbyCluster) {
        return ulRet;
    }

    ulRet = updateAllChildProcState(PROC_TASKS_BEFORE_UPDATE_SNAPSHOT);
    if (ulRet != EC_SUCCESS) {
        return ulRet;
    }

    ulRet = PerformTasksAfterRestoreCol(PARENT_PROC_SLOTID, PROC_TASKS_BEFORE_UPDATE_SNAPSHOT);
    if (ulRet != EC_SUCCESS) {
        LOGINFO("Failed to perform actions after restoring col for child %d, msg: %s",
                PARENT_PROC_SLOTID, getErrmsg(ulRet));
        return ulRet;
    }

    /* wait for all process finish tasks. */
    return waitForAllProcState(PROC_TASKS_DONE_BEFORE_UPDATE_SNAPSHOT, threadCtx);
}

/*
 * performRestoreParallely
 *
 * perform restore parallely
 * threadCtx
 * @return
 */
ERROR_CODE performRestoreParallely(THREAD_CONTEXT *threadCtx)
{
    ERROR_CODE ulRet = EC_SUCCESS;
    NODE_METADATA *meta = NULL;

    if (clioptions.bMaster == false) {
        LOGROACHOP("Restore operation started in agent [pid : %d]", getpid());
    }

    if (clioptions.bRestoreNewCluster == false) {
        meta = agentGlobals->nodeMetadata;
    } else {
        meta = agentGlobals->currNodeMetadata;
        ulRet = CheckMedadataCount(meta);
        if (ulRet != EC_SUCCESS) {
            return ulRet;
        }
    }

    /* for read only standby cluster. */
    ulRet = MkDirRestoredDelayRecoveryFiles();
    if (EC_SUCCESS != ulRet) {
        return ulRet;
    }

    ulRet = CreateFileListDir();
    if (ulRet != EC_SUCCESS) {
        LOGINFO("Failed to create file list folder when perform restore!");
        return ulRet;
    }

    ulRet = PerformRestoreTasks(threadCtx, meta);
    if (ulRet != EC_SUCCESS) {
        return ulRet;
    }

    return RemoveFilesAfterRestore(meta);
}

ERROR_CODE restoreTableFilesList(char *nbupath, SOCK sockfd)
{
    parray *fileList = NULL;
    ERROR_CODE ec = EC_SUCCESS;

    if (agentGlobals == NULL) {
        LOGERROR("Global variable [agentGlobals] is NULL when restore table file list.");
        return EC_INVALID_ARGUMENT;
    }
    media::StorageManager *smgr = agentGlobals->smgr;

    if (smgr == NULL) {
        LOGERROR("Storage manager pointer is NULL");
        return EC_INVALID_ARGUMENT;
    }
    fileList = smgr->ListFiles(NULL, nbupath, NULL);
    if (fileList == NULL) {
        LOGERROR("Failed to get file list for table level restore");
        ec = EC_SMGR_FAILED;
    }

    ec = RestoreDataFileFromFileList(fileList);
    parrayFree(fileList);
    UNUSED(ec);
    return ec;
}
/*lint -restore */
