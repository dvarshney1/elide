/*
 * Copyright 2020, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.core;

import com.yahoo.elide.ElideResponse;

import java.security.Principal;
import java.util.Map;
import java.util.UUID;

/**
 * Default NoopQuery Logger Implementation for Elide
 */
public class NoopQueryLogger implements QueryLogger {

    @Override
    public void acceptQuery(UUID queryId, Principal user, Map<String, String> headers, String apiVer, String apiQuery) {
        //does nothing
    }

    @Override
    public void processQuery(UUID queryId, QueryDetail qd) {
        //does nothing
    }

    @Override
    public void cancelQuery(UUID queryId) {
        //does nothing
    }

    @Override
    public void completeQuery(UUID queryId, ElideResponse response) {
        //does nothing
    }
}
