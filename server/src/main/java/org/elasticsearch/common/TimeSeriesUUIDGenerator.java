/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import java.util.Base64;

class TimeSeriesUUIDGenerator extends TimeBasedUUIDGenerator {

    @Override
    public String getBase64UUID() {
        final int sequenceId = sequenceNumber.incrementAndGet() & 0xffffff;
        long currentTimeMillis = currentTimeMillis();

        long timestamp = this.lastTimestamp.updateAndGet(lastTimestamp -> {
            // Don't let timestamp go backwards, at least "on our watch" (while this JVM is running). We are
            // still vulnerable if we are shut down, clock goes backwards, and we restart... for this we
            // randomize the sequenceNumber on init to decrease chance of collision:
            long nonBackwardsTimestamp = Math.max(lastTimestamp, currentTimeMillis);

            if (sequenceId == 0) {
                // Always force the clock to increment whenever sequence number is 0, in case we have a long
                // time-slip backwards:
                nonBackwardsTimestamp++;
            }

            return nonBackwardsTimestamp;
        });

        final byte[] uuidBytes = new byte[15];
        int i = 0;

        uuidBytes[i++] = (byte) (timestamp >>> 40); // changes every 35 years
        uuidBytes[i++] = (byte) (timestamp >>> 32); // changes every ~50 days
        uuidBytes[i++] = (byte) (timestamp >>> 24); // changes every ~4.5h

        byte[] macAddress = macAddress();
        assert macAddress.length == 6;
        System.arraycopy(macAddress, 0, uuidBytes, i, macAddress.length);
        i += macAddress.length;

        uuidBytes[i++] = (byte) (timestamp >>> 16); // changes every ~65 secs
        uuidBytes[i++] = (byte) (sequenceId >>> 16); // changes every 65k docs
        uuidBytes[i++] = (byte) (timestamp >>> 8);
        uuidBytes[i++] = (byte) (sequenceId >>> 8);

        uuidBytes[i++] = (byte) timestamp;
        uuidBytes[i++] = (byte) sequenceId;

        assert i == uuidBytes.length;

        return Base64.getUrlEncoder().withoutPadding().encodeToString(uuidBytes);
    }
}
