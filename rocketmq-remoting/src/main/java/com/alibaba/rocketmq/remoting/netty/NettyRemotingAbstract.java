/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.remoting.netty;

import com.alibaba.rocketmq.remoting.ChannelEventListener;
import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.common.Pair;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import com.alibaba.rocketmq.remoting.common.ServiceThread;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingSysResponseCode;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.*;


/**
 * @author shijia.wxr
 */
public abstract class NettyRemotingAbstract {
    private static final Logger plog = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);

    protected final Semaphore semaphoreOneway;

    protected final Semaphore semaphoreAsync;

    /**
     * 异步调用的响应信息
     * 发送请求之前构造好，放入map，等待对端返回结果
     */
    protected final ConcurrentHashMap<Integer /* opaque */, ResponseFuture> responseTable =
            new ConcurrentHashMap<Integer, ResponseFuture>(256);

    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;

    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
            new HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>>(64);

    protected final NettyEventExecuter nettyEventExecuter = new NettyEventExecuter();


    public abstract ChannelEventListener getChannelEventListener();


    public abstract RPCHook getRPCHook();


    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecuter.putNettyEvent(event);
    }

    /**
     * Netty事件处理器
     * 接收各种Netty事件，IDEL,CLOSE,CONNECT,EXCEPTION
     * 然后调用相应的Listener
     */
    class NettyEventExecuter extends ServiceThread {
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();
        private final int MaxSize = 10000;


        public void putNettyEvent(final NettyEvent event) {
            if (this.eventQueue.size() <= MaxSize) {
                this.eventQueue.add(event);
            } else {
                plog.warn("event queue size[{}] enough, so drop this event {}", this.eventQueue.size(),
                        event.toString());
            }
        }


        @Override
        public void run() {
            plog.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            while (!this.isStoped()) {
                try {
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                } catch (Exception e) {
                    plog.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            plog.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return NettyEventExecuter.class.getSimpleName();
        }
    }


    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }


    /**
     * 处理请求
     *
     * @param ctx
     * @param cmd
     */
    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        /**
         * 根据命令的Code查找对应的Processor，如果没找到，则使用默认的Processor
         */
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        final Pair<NettyRequestProcessor, ExecutorService> pair = (null == matched) ? this.defaultRequestProcessor : matched;

        if (pair != null) {
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        //钩子
                        RPCHook rpcHook = NettyRemotingAbstract.this.getRPCHook();

                        /**
                         * Before调用
                         */
                        if (rpcHook != null) {
                            rpcHook.doBeforeRequest(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd);
                        }

                        /**
                         * 处理请求，得到返回值,具体的请求逻辑都实现了NettyRequestProcessor接口
                         * Broker和NameServ不同逻辑实现不同
                         */
                        final RemotingCommand response = pair.getObject1().processRequest(ctx, cmd);

                        /**
                         * After调用
                         */
                        if (rpcHook != null) {
                            rpcHook.doAfterResponse(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd, response);
                        }

                        /**
                         * 如果不是OneWay，则把响应返回给客户端
                         */
                        if (!cmd.isOnewayRPC()) {
                            if (response != null) {

                                response.setOpaque(cmd.getOpaque());
                                response.markResponseType();

                                try {
                                    ctx.writeAndFlush(response);
                                } catch (Throwable e) {
                                    plog.error("process request over, but response failed", e);
                                    plog.error(cmd.toString());
                                    plog.error(response.toString());
                                }
                            } else {
                            }
                        }
                    } catch (Throwable e) {
                        plog.error("process request exception", e);
                        plog.error(cmd.toString());

                        /**
                         * 出错则返回错误响应
                         */
                        if (!cmd.isOnewayRPC()) {
                            final RemotingCommand response =
                                    RemotingCommand.createResponseCommand(
                                            RemotingSysResponseCode.SYSTEM_ERROR,//
                                            RemotingHelper.exceptionSimpleDesc(e));
                            response.setOpaque(cmd.getOpaque());
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            };

            /**
             * 提交给Processor对应的线程池处理
             */
            try {
                pair.getObject2().submit(run);
            } catch (RejectedExecutionException e) {
                /**
                 * 如果队列满了，服务降级
                 */
                if ((System.currentTimeMillis() % 10000) == 0) {
                    plog.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) //
                            + ", too many requests and system thread pool busy, RejectedExecutionException " //
                            + pair.getObject2().toString() //
                            + " request code: " + cmd.getCode());
                }

                if (!cmd.isOnewayRPC()) {
                    final RemotingCommand response =
                            RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                                    "too many requests and system thread pool busy, please try another server");
                    response.setOpaque(cmd.getOpaque());
                    ctx.writeAndFlush(response);
                }
            }
        } else {
            /**
             * 不支持的Code
             */
            String error = " request type " + cmd.getCode() + " not supported";

            final RemotingCommand response =
                    RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED,
                            error);
            response.setOpaque(cmd.getOpaque());
            ctx.writeAndFlush(response);

            plog.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
        }
    }


    /**
     * 处理Response命令
     * 如果是对端返回的数据，则执行这个方法，
     * 找到对应的ResponseFuture，然后执行其Callback方法或者设置Response内容
     *
     * @param ctx
     * @param cmd
     */
    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        /**
         * 找到对应的Response
         */
        final ResponseFuture responseFuture = responseTable.get(cmd.getOpaque());

        if (responseFuture != null) {
            responseFuture.setResponseCommand(cmd);

            //释放占用的信号量
            responseFuture.release();

            /**
             * 移除
             */
            responseTable.remove(cmd.getOpaque());
            /**
             * 如果有回调，则执行回调方法
             */
            if (responseFuture.getInvokeCallback() != null) {
                boolean runInThisThread = false;
                ExecutorService executor = this.getCallbackExecutor();
                if (executor != null) {
                    try {
                        executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    responseFuture.executeInvokeCallback();
                                } catch (Throwable e) {
                                    plog.warn("excute callback in executor exception, and callback throw", e);
                                }
                            }
                        });
                    } catch (Exception e) {
                        runInThisThread = true;
                        plog.warn("excute callback in executor exception, maybe executor busy", e);
                    }
                } else {
                    runInThisThread = true;
                }

                if (runInThisThread) {
                    try {
                        responseFuture.executeInvokeCallback();
                    } catch (Throwable e) {
                        plog.warn("executeInvokeCallback Exception", e);
                    }
                }

            } else {
                /**
                 * 没有回调则设置Response
                 */
                responseFuture.putResponse(cmd);
            }
        } else {
            plog.warn("receive response, but not matched any request, "
                    + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            plog.warn(cmd.toString());
        }
    }


    /**
     * 处理接收到的消息
     * 分为request_command和response_command
     *
     * @param ctx
     * @param msg 请求消息
     * @throws Exception
     */
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        final RemotingCommand cmd = msg;
        if (cmd != null) {
            switch (cmd.getType()) {
                case REQUEST_COMMAND:
                    processRequestCommand(ctx, cmd);
                    break;
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, cmd);
                    break;
                default:
                    break;
            }
        }
    }


    abstract public ExecutorService getCallbackExecutor();


    /**
     * 处理ResponseTable，如果超时则移除，同事触发Response中的回调。没有超时则继续等待
     */
    public void scanResponseTable() {
        Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();

            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                it.remove();
                try {
                    rep.executeInvokeCallback();
                } catch (Throwable e) {
                    plog.warn("scanResponseTable, operationComplete Exception", e);
                } finally {
                    rep.release();
                }

                plog.warn("remove timeout request, " + rep);
            }
        }
    }


    /**
     * 同步调用
     *
     * @param channel       通道
     * @param request       请求
     * @param timeoutMillis 超时时间
     * @return
     * @throws InterruptedException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     */
    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
                                          final long timeoutMillis) throws InterruptedException, RemotingSendRequestException,
            RemotingTimeoutException {
        try {
            /**
             * 创建响应，放入responseTable
             */
            final ResponseFuture responseFuture = new ResponseFuture(request.getOpaque(), timeoutMillis, null, null);
            this.responseTable.put(request.getOpaque(), responseFuture);

            /**
             * 继续发送请求让后面的业务逻辑处理，设置监听器，根据请求发送结果，设置response状态
             */
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                    } else {

                        responseFuture.setSendRequestOK(false);

                        responseTable.remove(request.getOpaque());

                        responseFuture.setCause(f.cause());

                        //同步调用，通过设置Response的方式，异步的话通过调用callback，参考下面的invokeAsyncImpl方法
                        //设置Response，这里很关键，否则下面的同步等待可能将会超时！！
                        responseFuture.putResponse(null);

                        plog.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                        plog.warn(request.toString());
                    }
                }
            });

            /**
             * 同步调用，所以
             * 等待Request处理结果，处理完之后会设置Response，下面的方法就会返回
             */
            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            if (null == responseCommand) {//超时
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(RemotingHelper.parseChannelRemoteAddr(channel),
                            timeoutMillis, responseFuture.getCause());
                } else {
                    throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel),
                            responseFuture.getCause());
                }
            }

            return responseCommand;

        } finally {
            //从ResponseTable中移除
            this.responseTable.remove(request.getOpaque());
        }
    }

    /**
     * 异步请求
     *
     * @param channel
     * @param request
     * @param timeoutMillis
     * @param invokeCallback
     * @throws InterruptedException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request,
                                final long timeoutMillis, final InvokeCallback invokeCallback) throws InterruptedException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        /**
         * 获取异步请求权限，通过semaphoreAsync来控制请求速度，避免请求速度过快！
         */
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {

            /**
             * 构造资源释放器，保证semaphoreAsync只释放一次
             */
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);

            /**
             * 构造response，放入responseTable,异步请求，所以需要设置Callback
             */
            final ResponseFuture responseFuture = new ResponseFuture(request.getOpaque(), timeoutMillis, invokeCallback, once);
            /**
             * 把ResponseFuture放入table,等待对端返回结果
             */
            this.responseTable.put(request.getOpaque(), responseFuture);


            try {
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        /**
                         * 发送成功，设置状态
                         */
                        if (f.isSuccess()) {
                            responseFuture.setSendRequestOK(true);
                        } else {

                            /**
                             * 发送请求失败，则不等待对端返回，直接执行回调
                             */
                            responseFuture.setSendRequestOK(false);
                            responseFuture.putResponse(null);

                            responseTable.remove(request.getOpaque());

                            try {
                                /**
                                 * 调用回调
                                 */
                                responseFuture.executeInvokeCallback();
                            } catch (Throwable e) {
                                plog.warn("excute callback in writeAndFlush addListener, and callback throw", e);
                            } finally {
                                responseFuture.release();
                            }

                            plog.warn("send a request command to channel <{}> failed.",
                                    RemotingHelper.parseChannelRemoteAddr(channel));
                            plog.warn(request.toString());
                        }
                    }
                });
            } catch (Exception e) {
                responseFuture.release();
                plog.warn(
                        "send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel)
                                + "> Exception", e);
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            /**
             * 请求太快，异步请求数量达到上限
             */
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            } else {
                String info =
                        String
                                .format(
                                        "invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", //
                                        timeoutMillis,//
                                        this.semaphoreAsync.getQueueLength(),//
                                        this.semaphoreAsync.availablePermits()//
                                );
                plog.warn(info);
                plog.warn(request.toString());
                throw new RemotingTimeoutException(info);
            }
        }
    }

    /**
     * OneWay调用，即不需要响应的请求
     *
     * @param channel
     * @param request
     * @param timeoutMillis
     * @throws InterruptedException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request,
                                 final long timeoutMillis) throws InterruptedException, RemotingTooMuchRequestException,
            RemotingTimeoutException, RemotingSendRequestException {
        request.markOnewayRPC();

        /**
         * 获取资源
         */
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);

            try {
                /**
                 * 继续发送请求，让后面的handler处理
                 */
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        //释放权限
                        once.release();

                        if (!f.isSuccess()) {
                            plog.warn("send a request command to channel <" + channel.remoteAddress()
                                    + "> failed.");
                            plog.warn(request.toString());
                        }
                    }
                });
            } catch (Exception e) {
                once.release();
                plog.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            } else {
                String info =
                        String
                                .format(
                                        "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", //
                                        timeoutMillis,//
                                        this.semaphoreAsync.getQueueLength(),//
                                        this.semaphoreAsync.availablePermits()//
                                );
                plog.warn(info);
                plog.warn(request.toString());
                throw new RemotingTimeoutException(info);
            }
        }
    }
}
