package metainforequester

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"
)

const mseKeyLifetime = 60 * time.Second

type mseKeyPair struct {
	private *big.Int
	public  [mseKeyLen]byte
	expires time.Time
}

type mseKeys struct {
	mu       sync.Mutex
	now      func() time.Time
	lifetime time.Duration
	current  *mseKeyPair
}

var sharedMSEKeys = &mseKeys{now: time.Now, lifetime: mseKeyLifetime}

func (k *mseKeys) get() (*mseKeyPair, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	now := k.now()
	if k.current != nil && now.Before(k.current.expires) {
		return k.current, nil
	}

	var seed [20]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil, err
	}

	pair := &mseKeyPair{private: new(big.Int).SetBytes(seed[:]), expires: now.Add(k.lifetime)}
	new(big.Int).Exp(mseGenerator, pair.private, msePrime).FillBytes(pair.public[:])
	k.current = pair

	return pair, nil
}
