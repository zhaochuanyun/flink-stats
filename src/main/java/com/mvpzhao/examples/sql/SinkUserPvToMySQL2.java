package com.mvpzhao.examples.sql;

import com.mvpzhao.examples.pojo.UserPvEntity;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


public class SinkUserPvToMySQL2 extends RichSinkFunction<UserPvEntity> {

    PreparedStatement ps;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "replace into t_user_pv(pvtime,userId, pvcount) values(?, ?, ?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     */
    @Override
    public void invoke(UserPvEntity userPvEntity, Context context) throws Exception {
        //组装数据，执行插入操作
        ps.setTimestamp(1, userPvEntity.getTime());
        ps.setString(2, userPvEntity.getUserId());
        ps.setLong(3, userPvEntity.getPvcount());

        ps.executeUpdate();
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/flinkmysql?useUnicode=true&characterEncoding=UTF-8", "root", "MVPzhao@24");
        } catch (Exception e) {
            System.err.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }
}
