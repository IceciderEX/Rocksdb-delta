ERROR_CODE PrepareDnBackupFileList(DATANODE_METADATA *datanode)
{
    ERROR_CODE ulRet = EC_SUCCESS;
    PREP_FILE_LIST_CTX *preFileListCtx = NULL;
    time_t beginTime = time(NULL);
    time_t currTime;
    char duration[TIMESTAMP_ARR_SIZE] = "----";
    errno_t rc = 0;

    if (BACKUP_MODE_TABLE == clioptions.eBackupType) {
        return EC_SUCCESS;
    }

    preFileListCtx = (PREP_FILE_LIST_CTX *)MALLOC(sizeof(PREP_FILE_LIST_CTX));
    if (NULL == preFileListCtx) {
        LOGERROR("Memory allocation failed for prepare filelist context.");
        return EC_MEMALLOC_FAILURE;
    }

    rc = memset_s(preFileListCtx, sizeof(PREP_FILE_LIST_CTX), 0, sizeof(PREP_FILE_LIST_CTX));
    securec_check_c(rc, "\0", "\0");
    preFileListCtx->localDataPath = datanode->localDataPath;
    preFileListCtx->instanceName = datanode->instanceName;
    preFileListCtx->dataNodeSize = &datanode->diskUsageDatanode;
    preFileListCtx->port = datanode->port;
    preFileListCtx->dataPrimiary = DATANODE_PRIMARY;
    preFileListCtx->conn = (GSCONN **)&(datanode->conn);
    preFileListCtx->startLsn = &datanode->startLsn;
    preFileListCtx->tli = &datanode->tli;
    preFileListCtx->bBackupStarted = &datanode->bStartGsBackup;
    preFileListCtx->instanceType = INSTANCE_DN;
    preFileListCtx->arcLogPath = datanode->arcLogPath;

    if (isResumeBkp()) {
        preFileListCtx->bkpState = &datanode->resumeBkpInfo.bkpState;
    }

    if (IS_INCREMENTAL_BACKUP_TYPE() || IS_INC_AT_TWO_PAHSE()) {
        int prior_inst_pos = -1;
        IdentifyMatchedPriorInstPos(datanode, &prior_inst_pos);

        if (-1 == prior_inst_pos) {
            /* This is required when standby did a switch over after the full backup */
            IdentifyMatchedPriorStandbyPos(datanode, &prior_inst_pos);
            if (-1 == prior_inst_pos) {
                LOGERROR("Datanode %s cannot be found in  %s.", datanode->instanceName, ROACHMASTERPRIORMETAFILE);
                FREE(preFileListCtx);
                return EC_AGENT_METADATA_FAILED;
            } else {
                preFileListCtx->priorStartLsn = agentGlobals->priorNodeMetadata->standbys[prior_inst_pos].startLsn;
                preFileListCtx->priorTli = &(agentGlobals->priorNodeMetadata->standbys[prior_inst_pos].tli);
            }
        } else {
            preFileListCtx->priorStartLsn = agentGlobals->priorNodeMetadata->datanodes[prior_inst_pos].startLsn;
            preFileListCtx->priorTli = &(agentGlobals->priorNodeMetadata->datanodes[prior_inst_pos].tli);
        }
    } else {
        preFileListCtx->priorStartLsn = InvalidXLogRecPtr;
        preFileListCtx->priorTli = NULL;
    }

    ulRet = PrepareDNCNInstanceFileList(preFileListCtx, (void *)datanode);
    if (ulRet != EC_SUCCESS) {
        LOGERROR("Failed to prepare backup filelist for instance:%s", datanode->localDataPath);
        FREE(preFileListCtx);
        return ulRet;
    }

    currTime = time(NULL);
    if (currTime >= beginTime) {
        /* Avoid user adjust system time during backup, if so, use default value of duration */
        getOperationDuration(currTime - beginTime, duration);
    }

    LOGINFO("Prepare metadata data for datanode %s finished, time:%s", datanode->instanceName, duration);
    FREE(preFileListCtx);
    return ulRet;
}

ERROR_CODE GenerateDnSoftLink(char *remoteIp, char *remotePath, char *nodeHostName, char *instanceName)
{
    ERROR_CODE ulRet = EC_SUCCESS;
    int nRet = 0;

    char parentDir[MAX_PATH_LEN] = "";
    char softLink[MAX_PATH_LEN] = "";
    char *bkpDir = NULL;
    auto bkpDirGuard = GS::MakeScopeGuard([&](){ FREE(bkpDir); });

    if (!clioptions.splitLocalDiskStorage) {
        return ulRet;
    }

    LOGINFO("Generate DN soft link for %s", instanceName);

    ulRet = MkdirRoachbackup(remoteIp, instanceName, remotePath, &bkpDir);
    if (ulRet != EC_SUCCESS) {
        LOGERROR("%s CreateSoftLinkInRemoteHost!", remotePath);
        return EC_DIR_CREATE_FAILED;
    }

    nRet = snprintf_s(parentDir, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/roach/%s/%s", clioptions.remoteMediaDestination,
        basename(globals->bkpKey), nodeHostName);
    securec_check_ss_c(nRet, "\0", "\0");
    ulRet = createDirectoryInRemoteHost(parentDir, remoteIp);
    if (ulRet != EC_SUCCESS) {
        LOGERROR("mkdir %s failed!", parentDir);
        return EC_DIR_CREATE_FAILED;
    }

    if (clioptions.isDrFineGrained) {
        nRet = snprintf_s(softLink, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/roach/%s/%s", clioptions.remoteMediaDestination,
                          basename(globals->bkpKey), instanceName);
    } else {
        nRet = snprintf_s(softLink, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/roach/%s/%s/%s", clioptions.remoteMediaDestination,
                  basename(globals->bkpKey), nodeHostName, instanceName);
    }
    securec_check_ss_c(nRet, "\0", "\0");
    ulRet = CreateSoftLinkInRemoteHost(softLink, remoteIp, bkpDir);
    if (ulRet != EC_SUCCESS) {
        LOGERROR("%s CreateSoftLinkInRemoteHost!", remotePath);
        return EC_DIR_CREATE_FAILED;
    }
    return ulRet;
}

STATIC void GetDataPath(string &dataPath, int index)
{
    if (IS_BINLOG_FINE_DR) {
        dataPath = BUCKET_PATH_PREFIX + std::string(BINLOG_DATA_FILE) + std::string(DIRECTORY_SEPARATOR) + GetDnInstanceName(index);
    } else {
        dataPath = BUCKET_PATH_PREFIX + to_string(index);
    }
}

STATIC DataNodeBaseInfo* GetMappedRemoteDnByType(const DATANODE_METADATA *localDn, char parentDir[MAX_PATH_LEN], int &bucketId, int index)
{
    int nRet = 0;
    DataNodeBaseInfo *mappedRemoteDn;
    if (IS_BINLOG_FINE_DR) {
        nRet = snprintf_s(parentDir, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/roach/%s/%s", clioptions.remoteMediaDestination,
            basename(globals->bkpKey), BINLOG_DATA_FILE);

        bucketId = index;
        mappedRemoteDn = GetMappedRemoteDn(agentGlobals->primaryDnToRemoteDnMap, bucketId);
    } else {
        nRet = snprintf_s(parentDir, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/roach/%s", clioptions.remoteMediaDestination,
            basename(globals->bkpKey));
        
        bucketId = localDn->buckets[index];
        mappedRemoteDn = GetMappedRemoteDn(agentGlobals->primaryBucketToRemoteDnMap, localDn->datanodeId, bucketId);
    }
    securec_check_ss_c(nRet, "\0", "\0");
    return mappedRemoteDn;
}

/*
 * Generate all buckets path for one DN, just like "GenerateDnSoftLink".
 */
ERROR_CODE GenerateDnBucketsSoftLink(const DATANODE_METADATA  *localDn, int index)
{
    ERROR_CODE ulRet = EC_SUCCESS;
    if (!clioptions.splitLocalDiskStorage) {
        return ulRet;
    }

    char parentDir[MAX_PATH_LEN] = "";
    char softLink[MAX_PATH_LEN] = "";
    char *bkpDir = NULL;
    int bucketId;

    DataNodeBaseInfo *mappedRemoteDn = GetMappedRemoteDnByType(localDn, parentDir, bucketId, index);
    LOGINFO("Generate DN buckets soft link for %s bucket %d", localDn->instanceName, bucketId);
    if (mappedRemoteDn == nullptr) {
        LOGINFO("Error: cannot get mapped remote DN for datanode %u, bucket %d.", localDn->datanodeId, bucketId);
        return EC_FG_DR_GET_MAPPED_DN_FAILED;
    }
    
    auto bkpDirGuard = GS::MakeScopeGuard([&](){ FREE(bkpDir); });
    const char *remoteIp = mappedRemoteDn->hostIp.data();
    string dataPath;
    /* for binlog: {localDataPath}/{backupKey}/binlog_data/{dnInstanceName}/{database}/{schema}/{table} */
    GetDataPath(dataPath, bucketId);
    ulRet = MkdirRoachbackup(remoteIp, dataPath.data(), mappedRemoteDn->localDataPath.data(), &bkpDir);
    if (ulRet != EC_SUCCESS) {
        LOGERROR("failed to create bucket(%d) source path on remote %s.", bucketId, remoteIp);
        return EC_DIR_CREATE_FAILED;
    }
    
    ulRet = createDirectoryInRemoteHost(parentDir, remoteIp);
    if (ulRet != EC_SUCCESS) {
        LOGERROR("mkdir link parentDir: %s failed!", parentDir);
        return EC_DIR_CREATE_FAILED;
    }
    
    int nRet = snprintf_s(softLink, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/roach/%s/%s", clioptions.remoteMediaDestination,
                      basename(globals->bkpKey), dataPath.data());
    securec_check_ss_c(nRet, "\0", "\0");
    ulRet = CreateSoftLinkInRemoteHost(softLink, remoteIp, bkpDir);
    if (ulRet != EC_SUCCESS) {
        LOGERROR("failed to create soft link on remote host: %s, linkpath: %s, srcpath: %s", remoteIp, softLink, bkpDir);
        return EC_DIR_CREATE_FAILED;
    }
    LOGINFO("Generate DN buckets soft link for %s bucket %d successfully", localDn->instanceName, bucketId);

    return EC_SUCCESS;
}

STATIC ERROR_CODE BackupDNFirstFiles(NODE_METADATA *meta, size_t count, int procIndexInInstance)
{
    ERROR_CODE ulRet = EC_SUCCESS;
    int prior_inst_pos = 0;

    IdentifyMatchedPriorInstPos(&meta->datanodes[count], &prior_inst_pos);

    /* This is required when standby did a switch over after the full backup */
    if (prior_inst_pos == -1) {
        IdentifyMatchedPriorStandbyPos(&meta->datanodes[count], &prior_inst_pos);
        if (prior_inst_pos == -1) {
            LOGERROR(
                "Datanode %s cannot be found in  %s.", meta->datanodes[count].instanceName, ROACHMASTERPRIORMETAFILE);
            return EC_AGENT_METADATA_FAILED;
        } else {
            /* data node instance must have been switched over. */
            ulRet = presetupAndBackupDataFiles((void *)&meta->datanodes[count], false, true, procIndexInInstance);
        }
    } else {
        ulRet = presetupAndBackupDataFiles((void *)&meta->datanodes[count], false, false, procIndexInInstance);
    }

    return ulRet;
}

STATIC ERROR_CODE BackupDNSecondFiles(NODE_METADATA *meta, int childSlotId, size_t count, int procIndexInInstance)
{
    ERROR_CODE ulRet = EC_SUCCESS;

    /* Resume backup. */
    if (isResumeBkp()) {
        if (meta->datanodes[count].resumeBkpInfo.bkpState >= BKP_ROW_FILE_FINISHED) {
            LOGINFO("[RESUME BACKUP INFO][RESUME BACKUP SKIP] instance[%s] skip the bkpState [BKP_ROW_FILE_FINISHED]", meta->datanodes[count].instanceName);
            /* Do not open transaction log. */
            ulRet = GetResumeBackupOption(
                clioptions.mediaType, meta->datanodes[count].instanceName, true, meta->datanodes[count].remoteIp,
                NULL, procIndexInInstance);
            if (ulRet != EC_SUCCESS) {
                return ulRet;
            }
        } else {
            /* Need to read transaction log in order to get the validity max rch file No. */
            ulRet = GetResumeBackupOption(
                clioptions.mediaType, meta->datanodes[count].instanceName, false, meta->datanodes[count].remoteIp,
                NULL, procIndexInInstance);
            if (ulRet != EC_SUCCESS) {
                return ulRet;
            }

            ulRet = presetupAndBackupDataFiles((void *)&meta->datanodes[count], false, false, procIndexInInstance);
            if (ulRet != EC_SUCCESS) {
                return ulRet;
            }
        }
    } else {
        ulRet = presetupAndBackupDataFiles((void *)&meta->datanodes[count], false, false, procIndexInInstance);
        if (ulRet != EC_SUCCESS) {
            return ulRet;
        }
    }

    return ulRet;
}

STATIC ERROR_CODE BackupLsmRowStageFiles(DATANODE_METADATA *meta, LSMBackupManager &manager)
{
    ERROR_CODE ulRet = EC_SUCCESS;
    ulRet = manager.InitInstanceInfo(meta, BKP_FILE_TYPE_LSM_LOCAL_ROWSTAGE);
    if (ulRet != EC_SUCCESS) {
        LOGERROR("Init instance info failed when do lsm csn log backup.");
        return ulRet;
    }
    ulRet = manager.BackupRowStageFile();
    if (ulRet != EC_SUCCESS) {
        LOGERROR("Backup lsm csn log files failed.");
        return ulRet;
    }
    return ulRet;
}

/**
 * backup datanode files
 * @param meta
 * @param childSlotId
 * @return EC_SUCCESS or failure
 */
STATIC ERROR_CODE PrepareDnInstancePaths(NODE_METADATA *meta, int dnIdx)
{
    ERROR_CODE ulRet = EC_SUCCESS;
    ulRet = CalcInstanceFileListPaths(meta->datanodes[dnIdx].instanceName,
        &meta->datanodes[dnIdx].fileListPath, &meta->datanodes[dnIdx].dirRecordFilePath,
        &meta->datanodes[dnIdx].softLinkRecordFilePath, BKP_FILE_TYPE_DATA);
    if (ulRet != EC_SUCCESS) {
        LOGERROR("Failed to calculate file list paths for instance: %s", meta->datanodes[dnIdx].instanceName);
        return ulRet;
    }

    ulRet = GenerateDnSoftLink(meta->datanodes[dnIdx].remoteIp, meta->datanodes[dnIdx].remotePath,
        meta->nodeHostName, meta->datanodes[dnIdx].instanceName);
    if (ulRet != EC_SUCCESS) {
        LOGERROR("Generate DN soft link in remote cluster failed.");
        return ulRet;
    }
    /* Reset the global vars for resume backup. */
    resetInstanceInfo();
    initTaskInfo(agentGlobals->destShMeta->instDnInfo, PROGRESS_DATA_START_STATE);
    return EC_SUCCESS;
}

ERROR_CODE backupDNFiles(NODE_METADATA *meta, int childSlotId)
{
    ERROR_CODE ulRet = EC_SUCCESS;

    /* Skip Copy Datafiles in pitr-xlog backup */
    if (isResumeBkp() && clioptions.bIsPitrMode) {
        LOGINFO("Skip performBackup in Pitr Xlog backup");
        return EC_SUCCESS;
    }

    // For master data nodes copy data files
    int loopCount = meta->datanodeCount * clioptions.parallelPerInstance;
    for (size_t i = 0; i < (size_t)loopCount; i++) {
        int dnIdx = GetPhysicalInstIdx(i);
        int procIndexInInstance = GetProcIdxFromSlot(i);

        /* allotTasksForMyProcInInstance with procIndexInInstance=-1 is equivalent to allotInstanceForMyProc */
        if (!allotTasksForMyProcInInstance(i, agentGlobals->destShMeta->instDnInfo,
            agentGlobals->destShMeta->childProc[childSlotId],
            meta->datanodes[dnIdx].instanceName, procIndexInInstance)) {
            continue;
        }

        LOGINFO("Parallel backup: i=%zu, dnIdx=%d, procIndexInInstance=%d",
            i, dnIdx, procIndexInInstance);

        ulRet = waitForLeadInstPhaseDone((int)i, agentGlobals->destShMeta->instDnInfo);
        if (ulRet != EC_SUCCESS) {
            LOGERROR("waitForLeadInstPhaseDone failed for DN instance[%s]", meta->datanodes[dnIdx].instanceName);
            return ulRet;
        }

        ulRet = PrepareDnInstancePaths(meta, dnIdx);
        if (ulRet != EC_SUCCESS) {
            return ulRet;
        }
        if (agentGlobals->priorNodeMetadata != NULL) {
            ulRet = BackupDNFirstFiles(meta, dnIdx, procIndexInInstance);
            if (ulRet != EC_SUCCESS) {
                return ulRet;
            }
        } else {
            ulRet = BackupDNSecondFiles(meta, childSlotId, dnIdx, procIndexInInstance);
            if (ulRet != EC_SUCCESS) {
                return ulRet;
            }
        }
        LSMBackupManager manager;
        ulRet = BackupLsmRowStageFiles(&meta->datanodes[dnIdx], manager);
        if (ulRet != EC_SUCCESS) {
            return ulRet;
        }

        updateTaskInfo((int)i, agentGlobals->destShMeta->instDnInfo, PROGRESS_DATA_DONE_STATE);
        UPDATE_FINISHED_INSTANCES(childSlotId);
        setInstPhaseDone((int)i, agentGlobals->destShMeta->instDnInfo);
    }

    return ulRet;
}



