#include <QtTest>
#include <QStandardPaths>
#include <QWebSocketServer>
#include <QWebSocket>
#include "../model/datacenter.h"

class MessageStateTest : public QObject {
    Q_OBJECT
    model::DataCenter* data = nullptr;
    std::unique_ptr<QWebSocketServer> server;
    QWebSocket* peer = nullptr;
    std::unique_ptr<QSignalSpy> frames;
    model::Message pending(const QString& chat, const QString& client = "client") {
        model::Message message;
        message.chatSessionId = chat;
        message.clientMessageId = client;
        message.sender.userId = "sender";
        message.deliveryStatus = model::DeliveryStatus::PENDING;
        return message;
    }
private slots:
    void initTestCase() {
        QStandardPaths::setTestModeEnabled(true);
        QCoreApplication::setApplicationName("SoarIM-Message-State-Tests");
        server = std::make_unique<QWebSocketServer>("test-gateway", QWebSocketServer::NonSecureMode);
        QVERIFY(server->listen(QHostAddress::LocalHost, 0));
        qputenv("SOARIM_WEBSOCKET_URL", ("ws://127.0.0.1:" + QString::number(server->serverPort())).toUtf8());
        connect(server.get(), &QWebSocketServer::newConnection, this, [this]() {
            peer = server->nextPendingConnection();
            frames = std::make_unique<QSignalSpy>(peer, &QWebSocket::binaryMessageReceived);
        });
        data = model::DataCenter::getInstance();
    }
    void receiptBeforeHttpDoesNotRegress() {
        auto message = pending("out-of-order");
        data->addMessage(message);
        data->updateMessageStatus("client", "server", model::DeliveryStatus::DELIVERED);
        data->updateMessageStatus("client", "server", model::DeliveryStatus::SENT);
        data->updateMessageStatus("client", "", model::DeliveryStatus::FAILED, "late error");
        const auto last = data->getRecentMessageList(message.chatSessionId)->back();
        QCOMPARE(last.deliveryStatus, model::DeliveryStatus::DELIVERED);
        QCOMPARE(last.messageId, QString("server"));
    }
    void duplicatePushIsNotARecipientReceipt() {
        auto message = pending("duplicate", "duplicate-client");
        data->addMessage(message);
        message.messageId = "persisted";
        QVERIFY(!data->addMessage(message));
        QCOMPARE(data->getRecentMessageList(message.chatSessionId)->size(), 1);
        QCOMPARE(data->getRecentMessageList(message.chatSessionId)->front().deliveryStatus,
                 model::DeliveryStatus::SENT);
    }
    void idsAreScopedToSender() {
        auto message = pending("different-senders", "shared-id");
        data->addMessage(message);
        message.sender.userId = "other-sender";
        QVERIFY(data->addMessage(message));
        QCOMPARE(data->getRecentMessageList(message.chatSessionId)->size(), 2);
    }
    void historyLagKeepsLiveMessageAndReceipt() {
        auto message = pending("history-lag", "history-client");
        data->addMessage(message);
        data->updateMessageStatus(message.clientMessageId, "history-server", model::DeliveryStatus::DELIVERED);
        auto response = std::make_shared<bite_im::GetRecentMsgRsp>();
        data->resetRecentMessageList(message.chatSessionId, response);
        QCOMPARE(data->getRecentMessageList(message.chatSessionId)->size(), 1);
        QCOMPARE(data->getRecentMessageList(message.chatSessionId)->front().deliveryStatus,
                 model::DeliveryStatus::DELIVERED);
    }
    void realWebsocketSendsReceiptForAcceptedAndDuplicateMessages() {
        data->resetLoginSessionId("test-token");
        data->initWebsocket();
        QTRY_VERIFY(peer != nullptr);
        QTRY_VERIFY(frames->count() > 0);
        QProtobufSerializer serializer;
        bite_im::ClientAuthenticationReq auth;
        QVERIFY(auth.deserialize(&serializer, frames->takeFirst().front().toByteArray()));
        QCOMPARE(auth.sessionId(), QString("test-token"));
        bite_im::NotifyMessage accepted;
        accepted.setNotifyType(bite_im::NotifyTypeGadget::NotifyType::AUTHENTICATION_NOTIFY);
        bite_im::NotifyAuthentication result;
        result.setSuccess(true);
        accepted.setAuthentication(result);
        QSignalSpy connected(data, &model::DataCenter::realtimeConnectionChanged);

        // Preload the conversation so this network-only test needs no HTTP server.
        data->addMessage(pending("ws-chat", "seed"));
        bite_im::MessageInfo info;
        info.setMessageId("ws-message");
        info.setClientMessageId("ws-client");
        info.setChatSessionId("ws-chat");
        bite_im::UserInfo sender;
        sender.setUserId("peer");
        info.setSender(sender);
        bite_im::StringMessageInfo text;
        text.setContent("hello");
        bite_im::MessageContent content;
        content.setMessageType(bite_im::MessageTypeGadget::MessageType::STRING);
        content.setStringMessage(text);
        info.setMessage(content);
        bite_im::NotifyNewMessage newMessage;
        newMessage.setMessageInfo(info);
        bite_im::NotifyMessage notification;
        notification.setNotifyType(bite_im::NotifyTypeGadget::NotifyType::CHAT_MESSAGE_NOTIFY);
        notification.setNewMessageInfo(newMessage);
        peer->sendBinaryMessage(notification.serialize(&serializer));
        QTest::qWait(30);
        QCOMPARE(frames->count(), 0); // no ACK before authentication succeeds
        peer->sendBinaryMessage(accepted.serialize(&serializer));
        QTRY_VERIFY(connected.count() > 0);
        QTRY_VERIFY(frames->count() > 0);
        bite_im::ClientAuthenticationReq earlyAck;
        QVERIFY(earlyAck.deserialize(&serializer, frames->takeFirst().front().toByteArray()));
        QCOMPARE(earlyAck.deliveryAck().messageId(), QString("ws-message"));
        for (int i = 0; i < 2; ++i) {
            peer->sendBinaryMessage(notification.serialize(&serializer));
            QTRY_VERIFY(frames->count() > 0);
            bite_im::ClientAuthenticationReq ack;
            QVERIFY(ack.deserialize(&serializer, frames->takeFirst().front().toByteArray()));
            QVERIFY(ack.hasDeliveryAck());
            QCOMPARE(ack.deliveryAck().messageId(), QString("ws-message"));
            QVERIFY(ack.sessionId().isEmpty());
        }
        QCOMPARE(data->getRecentMessageList("ws-chat")->size(), 2);
    }
    void rejectedAuthenticationClearsSessionAndStopsRetry() {
        QVERIFY(peer);
        QProtobufSerializer serializer;
        bite_im::NotifyAuthentication rejected;
        rejected.setSuccess(false);
        bite_im::NotifyMessage notification;
        notification.setNotifyType(bite_im::NotifyTypeGadget::NotifyType::AUTHENTICATION_NOTIFY);
        notification.setAuthentication(rejected);
        peer->sendBinaryMessage(notification.serialize(&serializer));
        QTRY_VERIFY(data->getLoginSessionId().isEmpty());
        data->closeWebsocket();
    }
};
QTEST_MAIN(MessageStateTest)
#include "message_state_test.moc"
