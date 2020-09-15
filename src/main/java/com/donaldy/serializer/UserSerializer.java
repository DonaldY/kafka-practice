package com.donaldy.serializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author donald
 * @date 2020/09/15
 */
public class UserSerializer implements Serializer<User> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, User user) {
        try {
            // 如果数据是null,则返回null
            if (user == null) return null;

            Integer userId = user.getUserId();
            String username = user.getUsername();

            int length = 0;
            byte[] bytes = null;
            if (null != username) {
                bytes = username.getBytes("utf-8");
                length = bytes.length;

            }

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + length);
            buffer.putInt(userId);
            buffer.putInt(length);
            buffer.put(bytes);


            return buffer.array();

        } catch (UnsupportedEncodingException e) {

            throw new SerializationException("序列化数据异常");

        }
    }

    @Override
    public void close() {

    }
}
