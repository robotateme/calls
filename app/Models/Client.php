<?php

declare(strict_types=1);

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

/**
 * @property int $id
 * @property string $phone
 */
final class Client extends Model
{
    protected $fillable = [
        'phone',
    ];
}
