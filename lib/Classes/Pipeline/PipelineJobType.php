<?php
/**
 * Class PipelineJobType
 *
 * @package TenupETL\Classes\Pipeline
 */

namespace TenupETL\Classes\Pipeline;

enum PipelineJobType: string {
	case Extract   = 'extract';
	case Transform = 'transform';
	case Load      = 'load';
}
