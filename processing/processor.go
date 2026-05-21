package processing

import (
	"github.com/imgproxy/imgproxy/v4/auximageprovider"
	"github.com/imgproxy/imgproxy/v4/imagedata"
	"github.com/imgproxy/imgproxy/v4/processing/svg"
	"github.com/imgproxy/imgproxy/v4/security"
)

// Processor is responsible for processing images according to the given configuration.
type Processor struct {
	config            *Config
	securityChecker   *security.Checker
	watermarkProvider auximageprovider.Provider
	imageDataFactory  *imagedata.Factory
	svg               *svg.Processor
}

// New creates a new Processor instance with the given configuration and watermark provider
func New(
	config *Config,
	securityChecker *security.Checker,
	watermark auximageprovider.Provider,
	idf *imagedata.Factory,
) (*Processor, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &Processor{
		config:            config,
		securityChecker:   securityChecker,
		watermarkProvider: watermark,
		imageDataFactory:  idf,
		svg:               svg.New(&config.Svg),
	}, nil
}
