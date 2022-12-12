package com.zgq.spark.produce;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @classname SocketProducer
 * @date 2022年12月7日17:50:33
 */
public class SocketProducer {
    public static void main(String[] args) {
        try {
            ServerSocket serverSocket = new ServerSocket(8020);
            System.out.println("启动 server ....");
            Socket socket = serverSocket.accept();
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            String response = "";

            //每 2s 发送一次消息
            Random r = new Random();   //不传入种子
            String[] lang = {"flink", "spark", "hadoop", "hive", "hbase", "impala", "presto", "superset", "nbi"};

            while (true) {
                List<String> list = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    list.add(lang[r.nextInt(lang.length)]);
                }
                response = String.join(" ", list) + "\n";
                System.out.println(response);
                try {
                    bw.write(response);
                    bw.flush();
                } catch (Exception ex) {
                    System.out.println(ex.getMessage());
                    System.exit(0);
                }
                Thread.sleep(1000);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}
