//go:build darwin && cgo

package auth

/*
#cgo LDFLAGS: -framework Security -framework CoreFoundation
#include <Security/Security.h>
#include <CoreFoundation/CoreFoundation.h>
#include <stdlib.h>
#include <string.h>

static int tempo_no_ui = 0;
static OSStatus tempo_disable_ui(void) { OSStatus s=SecKeychainSetUserInteractionAllowed(false); if(s==errSecSuccess) tempo_no_ui=1; return s; }
static CFMutableDictionaryRef tempo_query(void) {
 CFMutableDictionaryRef q = CFDictionaryCreateMutable(NULL, 0, &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
 CFDictionarySetValue(q,kSecClass,kSecClassGenericPassword);
 CFDictionarySetValue(q,kSecAttrService,CFSTR("io.beene.tempo"));
 CFDictionarySetValue(q,kSecAttrAccount,CFSTR("harvest-token"));
 CFDictionarySetValue(q,kSecAttrSynchronizable,kCFBooleanFalse);
 if(tempo_no_ui) CFDictionarySetValue(q,kSecUseAuthenticationUI,kSecUseAuthenticationUIFail);
 return q;
}
static OSStatus tempo_get(void **out, CFIndex *length) {
 CFMutableDictionaryRef q=tempo_query();
 CFDictionarySetValue(q,kSecReturnData,kCFBooleanTrue);
 CFDictionarySetValue(q,kSecMatchLimit,kSecMatchLimitOne);
 CFTypeRef result=NULL;
 OSStatus status=SecItemCopyMatching(q,&result);CFRelease(q);
 if(status!=errSecSuccess) {if(result) CFRelease(result);return status;}
 if(!result || CFGetTypeID(result)!=CFDataGetTypeID()) {if(result) CFRelease(result);return errSecDecode;}
 CFDataRef data=(CFDataRef)result;*length=CFDataGetLength(data);
 if(*length<1 || *length>16384) {CFRelease(result);return errSecDecode;}
 *out=malloc((size_t)*length);
 if(!*out) {CFRelease(result);return errSecAllocate;}
 memcpy(*out,CFDataGetBytePtr(data),(size_t)*length);CFRelease(result);return errSecSuccess;
}
static OSStatus tempo_set(const void *bytes,CFIndex length) {
 CFMutableDictionaryRef q=tempo_query();
 CFDataRef data=CFDataCreate(NULL,bytes,length);
 CFMutableDictionaryRef attrs=CFDictionaryCreateMutable(NULL,0,&kCFTypeDictionaryKeyCallBacks,&kCFTypeDictionaryValueCallBacks);
 CFDictionarySetValue(attrs,kSecValueData,data);
 OSStatus status=SecItemUpdate(q,attrs);
 if(status==errSecItemNotFound) {
  CFDictionarySetValue(q,kSecValueData,data);
  CFDictionarySetValue(q,kSecAttrAccessible,kSecAttrAccessibleWhenUnlockedThisDeviceOnly);
  status=SecItemAdd(q,NULL);
  if(status==errSecDuplicateItem) {CFDictionaryRemoveValue(q,kSecValueData);CFDictionaryRemoveValue(q,kSecAttrAccessible);status=SecItemUpdate(q,attrs);}
 }
 CFRelease(attrs);CFRelease(data);CFRelease(q);return status;
}
static OSStatus tempo_delete(void) {CFMutableDictionaryRef q=tempo_query();OSStatus status=SecItemDelete(q);CFRelease(q);return status;}
static void tempo_free(void *p,CFIndex length) {if(p) {memset_s(p,(size_t)length,0,(size_t)length);free(p);}}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

type keychainStore struct{}

// NewStore does not access Keychain. Only explicit Get, Set, or Delete calls do.
func NewStore() Store { return keychainStore{} }
func keychainError(status C.OSStatus) error {
	if status == C.errSecItemNotFound {
		return ErrNotFound
	}
	return fmt.Errorf("macOS Keychain operation failed (status %d); set HARVEST_TOKEN as an alternative", int32(status))
}
func (keychainStore) Get() (string, error) {
	var p unsafe.Pointer
	var n C.CFIndex
	status := C.tempo_get(&p, &n)
	if status != C.errSecSuccess {
		return "", keychainError(status)
	}
	defer C.tempo_free(p, n)
	token, err := validateToken(string(C.GoBytes(p, C.int(n))))
	if err != nil {
		return "", errInvalidToken
	}
	return token, nil
}
func (keychainStore) Set(token string) error {
	token, err := validateToken(token)
	if err != nil {
		return err
	}
	p := C.CBytes([]byte(token))
	defer C.tempo_free(p, C.CFIndex(len(token)))
	status := C.tempo_set(p, C.CFIndex(len(token)))
	if status != C.errSecSuccess {
		return keychainError(status)
	}
	return nil
}
func (keychainStore) Delete() error {
	status := C.tempo_delete()
	if status != C.errSecSuccess && status != C.errSecItemNotFound {
		return keychainError(status)
	}
	return nil
}

// These are used only by the owned helper; NewStore itself never changes policy.
func NativeSupported() bool { return true }
func noninteractiveStore() (Store, error) {
	if C.tempo_disable_ui() != C.errSecSuccess {
		return nil, issue("keychain", unchanged())
	}
	return boundedNativeStore{keychainStore{}}, nil
}

type boundedNativeStore struct{ keychainStore }

func (s boundedNativeStore) Set(t string) error {
	if e := s.keychainStore.Set(t); e != nil {
		return issue("credential_write_unknown", Effects{"unknown", "unchanged"})
	}
	return nil
}
func (s boundedNativeStore) Delete() error {
	if e := s.keychainStore.Delete(); e != nil {
		return issue("credential_write_unknown", Effects{"unknown", "unchanged"})
	}
	return nil
}
