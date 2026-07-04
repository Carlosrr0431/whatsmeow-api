package main

import (
	"strings"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// shouldProcessChat indica si el evento corresponde a un chat 1:1 que el CRM procesa.
func shouldProcessChat(info types.MessageInfo) bool {
	if !skipGroupsAndBroadcast {
		return true
	}
	if info.IsGroup {
		return false
	}
	return shouldProcessChatJID(info.Chat.String())
}

func shouldProcessChatJID(chatJID string) bool {
	if !skipGroupsAndBroadcast {
		return true
	}
	if chatJID == "" {
		return true
	}
	return !isIgnoredChatJID(chatJID)
}

// isIgnoredChatJID detecta chats que el CRM no procesa (grupos, estados, newsletters).
func isIgnoredChatJID(chatJID string) bool {
	if strings.Contains(chatJID, "@g.us") {
		return true
	}
	if strings.Contains(chatJID, "@broadcast") {
		return true
	}
	if strings.HasSuffix(chatJID, "@newsletter") {
		return true
	}
	return false
}

// slimMediaMessage guarda solo el sub-mensaje de media (menos RAM que clonar el mensaje completo).
func slimMediaMessage(msg *waE2E.Message) *waE2E.Message {
	u := unwrapMessage(msg)
	if u == nil {
		return nil
	}
	slim := &waE2E.Message{}
	switch {
	case u.GetImageMessage() != nil:
		slim.ImageMessage = proto.Clone(u.GetImageMessage()).(*waE2E.ImageMessage)
	case u.GetVideoMessage() != nil:
		slim.VideoMessage = proto.Clone(u.GetVideoMessage()).(*waE2E.VideoMessage)
	case u.GetAudioMessage() != nil:
		slim.AudioMessage = proto.Clone(u.GetAudioMessage()).(*waE2E.AudioMessage)
	case u.GetDocumentMessage() != nil:
		slim.DocumentMessage = proto.Clone(u.GetDocumentMessage()).(*waE2E.DocumentMessage)
	case u.GetStickerMessage() != nil:
		slim.StickerMessage = proto.Clone(u.GetStickerMessage()).(*waE2E.StickerMessage)
	default:
		return proto.Clone(msg).(*waE2E.Message)
	}
	return slim
}
